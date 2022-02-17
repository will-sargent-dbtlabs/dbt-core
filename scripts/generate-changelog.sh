#!/bin/bash

read -p "version number: " version
read -p "Final release? (Y/N): " final

if [ "$final" == "N" ] || [ "$final" == "n" ]; then
    read -p "Enter pre-release (rc, etc): " prerelease
    full_version="$version-$prerelease"
#     changie batch $full_version --keep
#     if [ ! -d /.changes/$version ]; then
#         mkdir ./.changes/$version
#     fi

#     # TODO: not sure why there would not be any changelog files, but check in case
#     mv ./.changes/unreleased/* ./.changes/$version/

else
    full_version=$version
    # TODO: remove all prerelease lines from current changelog
    mv ./.changes/$version/* ./.changes/unreleased/
    changie batch $full_version
fi

python ./scripts/add-contributors.py $full_version

changie merge
