package volcengineprovider

import "testing"

func TestParseFrameRate(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  int
	}{
		{value: "24/1", want: 24},
		{value: "24000/1001", want: 24},
	} {
		got, err := parseFrameRate(test.value)
		if err != nil || got != test.want {
			t.Fatalf("parseFrameRate(%q) = %d, %v; want %d", test.value, got, err, test.want)
		}
	}
}
