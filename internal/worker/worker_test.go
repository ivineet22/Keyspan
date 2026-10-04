package worker

import "testing"

func TestAckWithoutSyncIsRejected(t *testing.T) {
	if err := acceptCursor(0, 5); err == nil {
		t.Fatal("acked a cursor that was not synced")
	}
	if err := acceptCursor(4, 5); err == nil {
		t.Fatal("acked a cursor short of the synced position")
	}
	if err := acceptCursor(5, 5); err != nil {
		t.Fatal(err)
	}
	if err := acceptCursor(9, 5); err != nil {
		t.Fatal(err)
	}
}
