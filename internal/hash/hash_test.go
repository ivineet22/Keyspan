package hash

import "testing"

func TestPartsCoverTheLine(t *testing.T) {
	parts := Parts(3)
	if len(parts) != 3 {
		t.Fatalf("got %d spans", len(parts))
	}
	if parts[0][0] != 0 || parts[2][1] != Space {
		t.Fatalf("ends = %v", parts)
	}
	for i := 1; i < len(parts); i++ {
		if parts[i][0] != parts[i-1][1] {
			t.Fatalf("gap at %d: %v", i, parts)
		}
		if parts[i][0] >= parts[i][1] {
			t.Fatalf("empty span %v", parts[i])
		}
	}
}

func TestKeySeparatesTenantFromKey(t *testing.T) {
	if Key("ab", "c") == Key("a", "bc") {
		t.Fatal("the zero byte between tenant and key is missing")
	}
	if Key("acme", "user-1") != 0xaa7a19bc {
		t.Fatalf("acme/user-1 = %08x", Key("acme", "user-1"))
	}
}
