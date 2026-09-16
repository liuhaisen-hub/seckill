.PHONY: config
# generate pkg/conf proto
config:
	buf generate --template buf.gen.config.yaml