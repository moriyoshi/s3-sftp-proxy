package main

import (
	"fmt"
	"io/ioutil"
	"net"

	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
)

type User struct {
	Name       string
	Password   string
	PublicKeys []ssh.PublicKey
}

type UserStore struct {
	Name     string
	Users    []*User
	usersMap map[string]*User
}

type UserInfo struct {
	Addr net.Addr
	User string
}

func (ui *UserInfo) String() string {
	return fmt.Sprintf("%s from %s", ui.User, ui.Addr.String())
}

type UserStores map[string]UserStore

func (us *UserStore) Add(u *User) {
	us.Users = append(us.Users, u)
	us.usersMap[u.Name] = u
}

func (us *UserStore) Lookup(name string) *User {
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

func buildUsersFromAuthConfigInplace(users []*User, aCfg *AuthConfig) ([]*User, error) {
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
		users = append(users, &User{
			Name:       name,
			Password:   params.Password,
			PublicKeys: pubKeys,
		})
	}
	return users, nil
}

func buildUsersFromAuthConfig(users []*User, aCfg *AuthConfig) ([]*User, error) {
	switch aCfg.Type {
	case "inplace":
		return buildUsersFromAuthConfigInplace(users, aCfg)
	default:
		return users, fmt.Errorf("unknown auth config type: %s", aCfg.Type)
	}
}

func NewUserStoresFromConfig(cfg *S3SFTPProxyConfig) (UserStores, error) {
	uStores := UserStores{}
	for name, aCfg := range cfg.AuthConfigs {
		var err error
		var users []*User
		users, err = buildUsersFromAuthConfig(users, aCfg)
		if err != nil {
			return nil, err
		}
		usersMap := map[string]*User{}
		for _, u := range users {
			usersMap[u.Name] = u
		}
		uStores[name] = UserStore{Name: name, Users: users, usersMap: usersMap}
	}
	return uStores, nil
}
