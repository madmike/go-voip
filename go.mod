module github.com/madmike/go-voip

go 1.25.5

require (
	github.com/gorilla/websocket v1.5.3
	github.com/madmike/go-infra v0.0.1
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rs/zerolog v1.35.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/madmike/go-infra => ../infra
