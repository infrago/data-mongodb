module github.com/bamgoo/data-mongodb

go 1.25.3

require (
	github.com/bamgoo/bamgoo v0.0.0
	github.com/bamgoo/data v0.0.0
	go.mongodb.org/mongo-driver v1.17.6
)

replace github.com/bamgoo/bamgoo => ../bamgoo
replace github.com/bamgoo/data => ../data
