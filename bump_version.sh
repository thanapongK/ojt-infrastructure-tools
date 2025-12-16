#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 [major|minor|patch|beta]"
  exit 1
fi

# Get the bump type (major, minor, patch, or beta)
bump_type=$1

# Fetch the current version from a file (e.g., VERSION)
current_version=$(cat VERSION)
echo "Current version: $current_version"

# Function to increment the beta version
increment_beta() {
  if [[ $current_version == *"-beta."* ]]; then
    # If it's already a beta version, increment the beta number
    base_version=$(echo $current_version | sed 's/-beta\.[0-9]*//')
    beta_version=$(echo $current_version | grep -o 'beta\.[0-9]*' | grep -o '[0-9]*')
    beta_version=$((beta_version + 1))
    new_version="$base_version-beta.$beta_version"
  else
    # If it's not a beta version, convert it to beta.1
    new_version="$current_version-beta.1"
  fi
}

# Use Semver (Semantic Versioning) for bumping
if [[ $current_version == *"-beta."* ]]; then
  # Strip the beta suffix for version parts extraction
  base_version=$(echo $current_version | sed 's/-beta\.[0-9]*//')
  IFS='.' read -r -a version_parts <<< "$base_version"
else
  IFS='.' read -r -a version_parts <<< "$current_version"
fi

major=${version_parts[0]}
minor=${version_parts[1]}
patch=${version_parts[2]}

# Increment the version based on the bump type
case $bump_type in
  major)
    major=$((major + 1))
    minor=0
    patch=0
    new_version="$major.$minor.$patch"
    ;;
  minor)
    minor=$((minor + 1))
    patch=0
    new_version="$major.$minor.$patch"
    ;;
  patch)
    patch=$((patch + 1))
    new_version="$major.$minor.$patch"
    ;;
  beta)
    increment_beta
    ;;
  *)
    echo "Invalid bump type: $bump_type"
    exit 1
    ;;
esac

# Form the new version
echo "New version: $new_version"

# Update the version file
echo $new_version > VERSION

# Commit and tag
#git add VERSION
#git commit -m "Bump version to $new_version"
#git tag -a "v$new_version" -m "Version $new_version"
#git push origin main --tags
