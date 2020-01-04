package main

import (
	"fmt"
	"io/ioutil"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh"
)

// User user interface used to authenticate users using several methods
type User interface {
	ValidatePassword(pwd []byte) bool
	GetPublicKeys() []ssh.PublicKey
	GetName() string
	GetRootPath() string
	HasPublicKeys() bool
	HasPassword() bool
}

// UserStore user store
type UserStore struct {
	Name     string
	Users    []User
	usersMap map[string]User
}

// UserInfo user information
type UserInfo struct {
	Addr     net.Addr
	User     string
	RootPath string
}

func (ui *UserInfo) String() string {
	return fmt.Sprintf("%s from %s (root=%s)", ui.User, ui.Addr.String(), ui.RootPath)
}

// UserStores map of stores of users
type UserStores map[string]UserStore

// Add adds a user to current store
func (us *UserStore) Add(u User) {
	us.Users = append(us.Users, u)
	us.usersMap[u.GetName()] = u
}

// Lookup gets a user from current store
func (us *UserStore) Lookup(name string) User {
	u, _ := us.usersMap[name]
	return u
}

func parseAuthorizedKeys(pubKeys []ssh.PublicKey, pubKeyFileContent []byte) ([]ssh.PublicKey, error) {
	for len(pubKeyFileContent) > 0 {
		var pubKey ssh.PublicKey
		var err error
		pubKey, _, _, pubKeyFileContent, err = ssh.ParseAuthorizedKey(pubKeyFileContent)
		if err != nil {
			return pubKeys, err
		}
		pubKeys = append(pubKeys, pubKey)
	}
	return pubKeys, nil
}

func buildUsersFromAuthConfigInplace(users []User, aCfg *AuthConfig) ([]User, error) {
	for name, params := range aCfg.Users {
		var pubKeys []ssh.PublicKey
		if params.PublicKeys != "" {
			var err error
			pubKeys, err = parseAuthorizedKeys(pubKeys, []byte(params.PublicKeys))
			if err != nil {
				return users, errors.Wrapf(err, `user "%s"`, name)
			}
		}
		if params.PublicKeyFile != "" {
			var err error
			pubKeysFileContent, err := ioutil.ReadFile(params.PublicKeyFile)
			if err != nil {
				return users, errors.Wrapf(err, `user "%s"`, name)
			}
			pubKeys, err = parseAuthorizedKeys(pubKeys, pubKeysFileContent)
			if err != nil {
				return users, errors.Wrapf(err, `user "%s"`, name)
			}
		}
		switch params.AuthenticationMethod {
		case "bcrypt":
			users = append(users, &UserBcryptPassword{UserWithPassword{
				name:       name,
				password:   params.Password,
				rootPath:   params.RootPath,
				publicKeys: pubKeys,
			},
			})
		default:
			users = append(users, &UserPlainTextPassword{UserWithPassword{
				name:       name,
				password:   params.Password,
				rootPath:   params.RootPath,
				publicKeys: pubKeys,
			},
			})
		}
	}
	return users, nil
}

func buildUsersFromAuthConfig(users []User, aCfg *AuthConfig) ([]User, error) {
	switch aCfg.Type {
	case "inplace":
		return buildUsersFromAuthConfigInplace(users, aCfg)
	default:
		return users, fmt.Errorf("unknown auth config type: %s", aCfg.Type)
	}
}

// NewUserStoresFromConfig creates a new user stores based on configuration passed as parameter
func NewUserStoresFromConfig(cfg *S3SFTPProxyConfig) (UserStores, error) {
	uStores := UserStores{}
	for name, aCfg := range cfg.AuthConfigs {
		var err error
		var users []User
		users, err = buildUsersFromAuthConfig(users, aCfg)
		if err != nil {
			return nil, err
		}
		usersMap := map[string]User{}
		for _, u := range users {
			usersMap[u.GetName()] = u
		}
		uStores[name] = UserStore{Name: name, Users: users, usersMap: usersMap}
	}
	return uStores, nil
}

// UserWithPassword user with password. Used as base struct for other users that has a password.
type UserWithPassword struct {
	name       string
	password   string
	rootPath   string
	publicKeys []ssh.PublicKey
}

// GetPublicKeys gets public keys
func (u *UserWithPassword) GetPublicKeys() []ssh.PublicKey {
	return u.publicKeys
}

// GetName gets user name
func (u *UserWithPassword) GetName() string {
	return u.name
}

// GetRootPath root path
func (u *UserWithPassword) GetRootPath() string {
	return u.rootPath
}

// HasPublicKeys wether the user has public keys or not
func (u *UserWithPassword) HasPublicKeys() bool {
	return u.publicKeys != nil
}

// HasPassword wether the user has a password or not
func (u *UserWithPassword) HasPassword() bool {
	return u.password != ""
}

// UserPlainTextPassword user with plain text password
type UserPlainTextPassword struct {
	UserWithPassword
}

// ValidatePassword validates a password
func (u *UserPlainTextPassword) ValidatePassword(pwd []byte) bool {
	return u.password != "" && u.password == string(pwd)
}

// UserBcryptPassword user with password encrypted using bcrypt
type UserBcryptPassword struct {
	UserWithPassword
}

// ValidatePassword validates a password
func (u *UserBcryptPassword) ValidatePassword(pwd []byte) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.password), pwd)
	return err == nil
}
