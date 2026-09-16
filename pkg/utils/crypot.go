package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
)

// GenerateSalt 生成指定长度随机盐，推荐16/32字节
func GenerateSalt(length int) (string, error) {
	if length <= 0 {
		return "", errors.New("salt length invalid")
	}
	buf := make([]byte, length)
	_, err := rand.Read(buf)
	if err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(buf), nil
}

// CreatePasswordHash 用用户独立盐加密密码
func CreatePasswordHash(plainPwd, salt string) string {
	// 组合：盐在前 + 密码
	saltPwd := append([]byte(salt), []byte(plainPwd)...)
	h := sha256.New()
	h.Write(saltPwd)
	return base64.RawStdEncoding.EncodeToString(h.Sum(nil))
}

// VerifyPassword 校验密码
func VerifyPassword(plainPwd, salt, hash string) bool {
	newHash := CreatePasswordHash(plainPwd, salt)
	// 时序安全对比，防止时序攻击
	return subtle.ConstantTimeCompare([]byte(newHash), []byte(hash)) == 1
}
