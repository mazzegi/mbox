package uuid

import (
	"crypto/rand"
	"fmt"
)

// MakeV4 generates a random V4 UUID
func MakeV4() (string, error) {
	u := [16]byte{}
	// Set all bits to randomly (or pseudo-randomly) chosen values.
	_, err := rand.Read(u[:])
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:]), nil
}

// MustMakeV4 generates a random V4 UUID and panics on fail
func MustMakeV4() string {
	id, err := MakeV4()
	if err != nil {
		panic(err)
	}
	return id
}
