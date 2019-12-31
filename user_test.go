package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserBcryptPasswordValidation(t *testing.T) {
	tests := []struct {
		enc      string
		pass     string
		expected bool
	}{
		{
			enc:      "$2a$04$IdGko3VpUeqY/HEFv5olLOa/E.dswOKxSEivXDSYnvXLWRQyJSFOi", // using bcrypt.MinCost
			pass:     "test",
			expected: true,
		},
		{
			enc:      "$2a$10$PSknC1YfCXVxZ.p84xSK8u3.n0mMcoBqiTaentkAEPPLnS3/jji1W", // using bcrypt.DefaultCost
			pass:     "test",
			expected: true,
		},
		{
			enc:      "$2a$10$PSknC1YfCXVxZ.p84xSK8u3.n0mMcoBqiTaentkAEPPLnS3/jji1W", // using bcrypt.DefaultCost
			pass:     "test2",
			expected: false,
		},
	}

	for _, test := range tests {
		u := &UserBcryptPassword{UserWithPassword{password: test.enc}}
		assert.Equal(t, test.expected, u.ValidatePassword([]byte(test.pass)))
	}
}

func TestUserPlainPasswordValidation(t *testing.T) {
	tests := []struct {
		enc      string
		pass     string
		expected bool
	}{
		{
			enc:      "test",
			pass:     "test",
			expected: true,
		},
		{
			enc:      "test",
			pass:     "test2",
			expected: false,
		},
	}

	for _, test := range tests {
		u := &UserPlainTextPassword{UserWithPassword{password: test.enc}}
		assert.Equal(t, test.expected, u.ValidatePassword([]byte(test.pass)))
	}
}

func TestNewUserStoresFromConfigSeveralTypes(t *testing.T) {
	stores, err := NewUserStoresFromConfig(&S3SFTPProxyConfig{
		AuthConfigs: map[string]*AuthConfig{
			"test": &AuthConfig{
				Type: "inplace",
				Users: map[string]AuthUser{
					"user1": AuthUser{
						Password: "test",
					},
					"user2": AuthUser{
						Password:             "test",
						AuthenticationMethod: "plain",
					},
					"user3": AuthUser{
						Password:             "$2a$04$IdGko3VpUeqY/HEFv5olLOa/E.dswOKxSEivXDSYnvXLWRQyJSFOi",
						AuthenticationMethod: "bcrypt",
					},
				},
			},
		},
	})
	us, _ := stores["test"]
	assert.NoError(t, err)
	assert.Equal(t, true, us.Lookup("user1").ValidatePassword([]byte("test")))
	assert.Equal(t, false, us.Lookup("user1").ValidatePassword([]byte("test2")))
	assert.Equal(t, true, us.Lookup("user2").ValidatePassword([]byte("test")))
	assert.Equal(t, false, us.Lookup("user2").ValidatePassword([]byte("test2")))
	assert.Equal(t, true, us.Lookup("user3").ValidatePassword([]byte("test")))
	assert.Equal(t, false, us.Lookup("user3").ValidatePassword([]byte("test2")))
}
