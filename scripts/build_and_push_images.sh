#!/bin/bash
# Script to build and push Docker images to multiple container registries

set -e

# Default values
IMAGE_NAME="beam-mcp-server"
VERSION=$(grep -oP 'version: "\K[^"]+' src/server/config.py 2>/dev/null || echo "1.0.0")
REGISTRY=""
PLATFORMS="linux/amd64,linux/arm64"
BUILD_ARGS=""
PUSH=false
LATEST=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --registry)
      REGISTRY="$2"
      shift 2
      ;;
    --version)
      VERSION="$2"
      shift 2
      ;;
    --platforms)
      PLATFORMS="$2"
      shift 2
      ;;
    --build-arg)
      BUILD_ARGS="$BUILD_ARGS --build-arg $2"
      shift 2
      ;;
    --push)
      PUSH=true
      shift
      ;;
    --latest)
      LATEST=true
      shift
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo "Build and push Docker images for Beam MCP Server"
      echo ""
      echo "Options:"
      echo "  --registry REGISTRY    Container registry (e.g., gcr.io/project-id)"
      echo "  --version VERSION      Image version tag (default: extracted from code or 1.0.0)"
      echo "  --platforms PLATFORMS  Target platforms (default: linux/amd64,linux/arm64)"
      echo "  --build-arg ARG        Pass build argument to Docker"
      echo "  --push                 Push images after building"
      echo "  --latest               Also tag and push as latest"
      echo "  --help                 Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate registry if pushing
if [ "$PUSH" = true ] && [ -z "$REGISTRY" ]; then
  echo "Error: --registry is required when using --push"
  exit 1
fi

# Set full image name
if [ -n "$REGISTRY" ]; then
  FULL_IMAGE_NAME="$REGISTRY/$IMAGE_NAME"
else
  FULL_IMAGE_NAME="$IMAGE_NAME"
fi

echo "Building $FULL_IMAGE_NAME:$VERSION..."

# Enable Docker BuildKit
export DOCKER_BUILDKIT=1

# Build the image
docker buildx create --name beam-mcp-builder --use --bootstrap || true
docker buildx build \
  --platform "$PLATFORMS" \
  $BUILD_ARGS \
  -t "$FULL_IMAGE_NAME:$VERSION" \
  $([ "$PUSH" = true ] && echo "--push") \
  $([ "$PUSH" = false ] && echo "--load") \
  .

# Tag and push latest if requested
if [ "$LATEST" = true ] && [ "$PUSH" = true ]; then
  echo "Tagging and pushing as latest..."
  docker tag "$FULL_IMAGE_NAME:$VERSION" "$FULL_IMAGE_NAME:latest"
  docker push "$FULL_IMAGE_NAME:latest"
fi

echo "Build completed successfully!"
if [ "$PUSH" = true ]; then
  echo "Images pushed to $FULL_IMAGE_NAME:$VERSION"
  [ "$LATEST" = true ] && echo "Images pushed to $FULL_IMAGE_NAME:latest"
else
  echo "Images built locally as $FULL_IMAGE_NAME:$VERSION"
fi

# List of registries for multi-registry push
if [ "$PUSH" = true ] && [ -n "$REGISTRY" ]; then
  # Example of pushing to multiple registries
  # Uncomment and modify as needed
  
  # REGISTRIES=(
  #   "gcr.io/your-project"
  #   "us-docker.pkg.dev/your-project/beam-mcp"
  #   "docker.io/yourusername"
  # )
  
  # for reg in "${REGISTRIES[@]}"; do
  #   if [ "$reg" != "$REGISTRY" ]; then
  #     echo "Pushing to additional registry: $reg"
  #     ALT_IMAGE_NAME="$reg/$IMAGE_NAME:$VERSION"
  #     docker tag "$FULL_IMAGE_NAME:$VERSION" "$ALT_IMAGE_NAME"
  #     docker push "$ALT_IMAGE_NAME"
  #     
  #     if [ "$LATEST" = true ]; then
  #       ALT_LATEST="$reg/$IMAGE_NAME:latest"
  #       docker tag "$FULL_IMAGE_NAME:$VERSION" "$ALT_LATEST"
  #       docker push "$ALT_LATEST"
  #     fi
  #   fi
  # done
fi 