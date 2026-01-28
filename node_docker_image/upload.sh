
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source $SCRIPT_DIR/../.env

# aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/s9d3x9f5
# docker tag conflux:dev public.ecr.aws/s9d3x9f5/conflux-massive-test/conflux-node:latest
# docker push public.ecr.aws/s9d3x9f5/conflux-massive-test/conflux-node:latest

echo $DOCKER_ACCESS_TOKEN | docker login --username $DOCKER_USERNAME --password-stdin
docker tag conflux:dev $DOCKER_USERNAME/conflux-node:latest
docker push $DOCKER_USERNAME/conflux-node:latest

# docker pull public.ecr.aws/s9d3x9f5/conflux-massive-test/conflux-node:latest