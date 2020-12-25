DIR = "proto"

.PHONY: gen_proto
gen_proto:
	@echo "generate protoc .pb.go"
	@protoc -I $(DIR) **/**.proto --go_out=plugins=grpc:$(DIR) --go_opt=paths=source_relative