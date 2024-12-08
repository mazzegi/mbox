package convert

import "testing"

func TestToBool(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		want bool
	}{
		{name: "bool true", v: true, want: true},
		{name: "bool false", v: false, want: false},
		{name: "string true", v: "true", want: true},
		{name: "string false", v: "false", want: false},
		{name: "string yes", v: "yes", want: true},
		{name: "string no", v: "no", want: false},
		{name: "string on", v: "on", want: true},
		{name: "string off", v: "off", want: false},
		{name: "int 1", v: 1, want: true},
		{name: "int 0", v: 0, want: false},
		{name: "float 1", v: 1.0, want: true},
		{name: "float 0", v: 0.0, want: false},
		{name: "nil", v: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToBool(tt.v); got != tt.want {
				t.Errorf("ToBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		want int
	}{
		{name: "int", v: 1, want: 1},
		{name: "float64", v: 1.5, want: 1},
		{name: "float32", v: float32(1.5), want: 1},
		{name: "string", v: "1", want: 1},
		{name: "bool true", v: true, want: 1},
		{name: "bool false", v: false, want: 0},
		{name: "nil", v: nil, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := ToInt(tt.v); got != tt.want {
				t.Errorf("ToInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestToFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected float64
		success  bool
	}{
		{
			name:     "int conversion",
			input:    10,
			expected: 10.0,
			success:  true,
		},
		{
			name:     "float conversion",
			input:    3.14,
			expected: 3.14,
			success:  true,
		},
		{
			name:     "string conversion",
			input:    "2.718",
			expected: 2.718,
			success:  true,
		},
		{
			name:     "invalid string conversion",
			input:    "not a number",
			expected: 0,
			success:  false,
		},
		{
			name:     "unsupported type conversion",
			input:    []int{1, 2, 3},
			expected: 0,
			success:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, success := ToFloat(test.input)
			if success != test.success {
				t.Errorf("expected success value of %v, but got %v", test.success, success)
			}
			if success && result != test.expected {
				t.Errorf("expected %v, but got %v", test.expected, result)
			}
		})
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "int conversion",
			input:    10,
			expected: "10",
		},
		{
			name:     "float conversion",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "bool conversion",
			input:    true,
			expected: "true",
		},
		{
			name:     "slice conversion",
			input:    []int{1, 2, 3},
			expected: "[1 2 3]",
		},
		{
			name: "struct conversion",
			input: struct {
				A int
				B string
			}{
				A: 10,
				B: "hello",
			},
			expected: "{10 hello}",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ToString(test.input)
			if result != test.expected {
				t.Errorf("expected %q, but got %q", test.expected, result)
			}
		})
	}
}
