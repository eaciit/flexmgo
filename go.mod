module github.com/ariefdarmawan/flexmgo

go 1.16

//replace git.kanosolution.net/kano/dbflex => D:\Coding\lib\dbflex

//replace github.com/sebarcode/codekit => D:\Coding\lib\codekit

//replace github.com/sebarcode/logger => D:\Coding\lib\logger

//replace github.com/ariefdarmawan/serde => D:\Coding\lib\serde

require (
	git.kanosolution.net/kano/dbflex v1.2.1
	github.com/ariefdarmawan/serde v0.1.0
	github.com/sebarcode/codekit v0.1.0
	github.com/sebarcode/logger v0.1.1
	github.com/smartystreets/goconvey v1.7.2
	go.mongodb.org/mongo-driver v1.9.1
)
