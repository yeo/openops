org := 5petal
name := web2ldap
repo := $(org)/$(name)
git_commit ?= $(shell git rev-parse HEAD)
image ?= ${repo}:$(git_commit)

build:
	docker build -t ${image} .
	docker push ${image}
	docker tag ${image} ${repo}:latest
	docker push ${repo}:latest

run:
	docker run -p 1760:1760 ${image}
