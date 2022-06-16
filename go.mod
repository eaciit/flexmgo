module github.com/ariefdarmawan/flexmgo

go 1.16

replace git.kanosolution.net/kano/dbflex => D:\Coding\lib\dbflex

replace github.com/sebarcode/codekit => D:\Coding\lib\codekit

replace github.com/sebarcode/logger => D:\Coding\lib\logger

replace github.com/ariefdarmawan/serde => D:\Coding\lib\serde

//replace git.kanosolution.net/kano/dbflex => D:\coding\lib\dbflex

require (
	git.kanosolution.net/kano/dbflex v1.1.0
	github.com/ariefdarmawan/serde v0.0.0-20220616152415-ac746173e86f
	github.com/sebarcode/codekit v0.0.0-20220616144406-d7c5cafaca19
	github.com/sebarcode/logger v0.0.0-00010101000000-000000000000
	github.com/smartystreets/goconvey v1.7.2
	go.mongodb.org/mongo-driver v1.9.1
)
