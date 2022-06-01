#!/usr/bin/env sh

TEST="$*"

if [ -z "$TEST" ]; then
    docker compose -f script/integration_test_compose.yml down
    docker compose -f script/integration_test_compose.yml up -d
    go test -race ./... -tags=e2e
    docker compose -f script/integration_test_compose.yml down
elif [ "$TEST" = "up" ]; then
    docker compose -f script/integration_test_compose.yml up -d
elif [ "$TEST" = "down" ]; then
    docker compose -f script/integration_test_compose.yml down
else
  echo "非法命令，只能是 make e2e [up | down]"
fi
