package config

//go:generate go-enum -f=$GOFILE --marshal
//ENUM(
//mongo
//rethink
//)
type DatabaseType uint
