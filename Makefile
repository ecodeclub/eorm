# 单元测试
ut:
	@go test -race ./...

setup:
	sh ./script/setup.sh

lint:
	golangci-lint run

# e2e 测试
# make e2e
# make e2e up 只启动测试环境
# make e2e down 只清理测试环境
e2e:
	sh ./script/integrate_test.sh $*