package controlplane

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

var (
	sha256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
)

type Action string

const (
	ActionCreateSeries    Action = "series.create"
	ActionCreateRevision  Action = "revision.create"
	ActionCreatePlan      Action = "generation_plan.create"
	ActionStartProduction Action = "production.start"
	ActionCreateRun       Action = "run.create"
	ActionCancelRun       Action = "run.cancel"
	ActionResumeRun       Action = "run.resume"
	ActionApproveG1       Action = "approval.g1"
	ActionApproveG2       Action = "approval.g2"
	ActionApproveQ1       Action = "approval.q1"
	ActionApproveG3       Action = "approval.g3"
)

var allowedRoles = map[Action]map[string]struct{}{
	ActionCreateSeries:    roles("CREATOR", "PRODUCER", "ADMIN"),
	ActionCreateRevision:  roles("CREATOR", "ARTIST", "PRODUCER", "ADMIN"),
	ActionCreatePlan:      roles("PRODUCER", "OPERATOR", "ADMIN"),
	ActionStartProduction: roles("PRODUCER", "OPERATOR", "ADMIN"),
	ActionCreateRun:       roles("ARTIST", "PRODUCER", "OPERATOR", "ADMIN"),
	ActionCancelRun:       roles("PRODUCER", "OPERATOR", "ADMIN"),
	ActionResumeRun:       roles("PRODUCER", "OPERATOR", "ADMIN"),
	ActionApproveG1:       roles("DIRECTOR", "PRODUCER"),
	ActionApproveG2:       roles("DIRECTOR", "PRODUCER"),
	ActionApproveQ1:       roles("REVIEWER", "DIRECTOR"),
	ActionApproveG3:       roles("DIRECTOR", "PRODUCER"),
}

func roles(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func authorize(action Action, actor Actor) error {
	actor.ActorID = strings.TrimSpace(actor.ActorID)
	actor.Role = strings.ToUpper(strings.TrimSpace(actor.Role))
	if actor.ActorID == "" || actor.Role == "" {
		return forbiddenError("actorId and role are required")
	}
	rolesForAction, ok := allowedRoles[action]
	if !ok {
		return forbiddenError("the requested action has no authorization policy")
	}
	if _, ok := rolesForAction[actor.Role]; !ok {
		return forbiddenError(fmt.Sprintf("role %q cannot perform %s", actor.Role, action))
	}
	return nil
}

func approvalAction(gate string) (Action, error) {
	switch strings.ToUpper(strings.TrimSpace(gate)) {
	case "G1":
		return ActionApproveG1, nil
	case "G2":
		return ActionApproveG2, nil
	case "Q1":
		return ActionApproveQ1, nil
	case "G3":
		return ActionApproveG3, nil
	default:
		return "", validationError("gate must be one of G1, G2, Q1, or G3")
	}
}

func validateUUID(name, value string) error {
	if _, err := uuid.Parse(value); err != nil {
		return validationError(name + " must be a UUID")
	}
	return nil
}

func validateSHA256(name, value string) error {
	if !sha256Pattern.MatchString(value) {
		return validationError(name + " must be a lowercase SHA-256 digest")
	}
	return nil
}

func validateArtifact(hash, uri string) error {
	if err := validateSHA256("artifactHash", hash); err != nil {
		return err
	}
	if uri != "cas://sha256/"+hash {
		return validationError("artifactUri must address artifactHash in CAS")
	}
	return nil
}

func requestDigest(value any) (string, error) {
	payload, err := CanonicalJSON(value)
	if err != nil {
		return "", fmt.Errorf("encode canonical request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validateRoute(route ModelRouteSnapshot) error {
	if err := validateUUID("providerProfileId", route.ProviderProfileID); err != nil {
		return err
	}
	if route.CapabilityAlias == "" || route.Provider == "" || route.ModelID == "" || route.RouteVersion == "" {
		return validationError("routeSnapshot requires capabilityAlias, provider, modelId, and routeVersion")
	}
	return validateSHA256("routeSnapshot.capabilityHash", route.CapabilityHash)
}

func validateVideoRoute(route ModelRouteSnapshot) error {
	if err := validateRoute(route); err != nil {
		return err
	}
	if route.CapabilityAlias != "video.primary" {
		return validationError("routeSnapshot.capabilityAlias must be video.primary")
	}
	return nil
}

func uniqueUUIDs(name string, values []string) error {
	if len(values) == 0 {
		return validationError(name + " must contain at least one item")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateUUID(name, value); err != nil {
			return err
		}
		if _, ok := seen[value]; ok {
			return validationError(name + " must not contain duplicates")
		}
		seen[value] = struct{}{}
	}
	return nil
}
