package model

import "testing"

func TestParseLag(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		wantNil bool
		wantLen int
	}{
		{
			name:    "normal",
			input:   `{"0":0,"1":80009,"2":0,"3":0,"4":80008}`,
			wantNil: false,
			wantLen: 5,
		},
		{
			name:    "empty string",
			input:   "",
			wantNil: true,
		},
		{
			name:    "invalid json",
			input:   "invalid",
			wantNil: true,
		},
		{
			name:    "all zero",
			input:   `{"0":0,"1":0}`,
			wantNil: false,
			wantLen: 2,
		},
		{
			name:    "single partition",
			input:   `{"3":99999}`,
			wantNil: false,
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseLag(tt.input)
			if tt.wantNil {
				if result != nil {
					t.Errorf("ParseLag(%q) = %v, want nil", tt.input, result)
				}
				return
			}
			if result == nil {
				t.Fatalf("ParseLag(%q) = nil, want non-nil", tt.input)
			}
			if len(result) != tt.wantLen {
				t.Errorf("ParseLag(%q) len = %d, want %d", tt.input, len(result), tt.wantLen)
			}
		})
	}
}

func TestParseLag_Values(t *testing.T) {
	input := `{"0":0,"1":80009,"2":0,"4":80008,"7":80019}`
	result := ParseLag(input)
	if result == nil {
		t.Fatal("ParseLag returned nil")
	}

	if result["1"] != 80009 {
		t.Errorf("partition 1 = %d, want 80009", result["1"])
	}
	if result["4"] != 80008 {
		t.Errorf("partition 4 = %d, want 80008", result["4"])
	}
	if result["7"] != 80019 {
		t.Errorf("partition 7 = %d, want 80019", result["7"])
	}
	if result["0"] != 0 {
		t.Errorf("partition 0 = %d, want 0", result["0"])
	}
}
