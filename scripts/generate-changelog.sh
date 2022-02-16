read -p "version number: " version
read -p "Final release? (Y/N): " final

if [ "$final" == "N" ] || [ "$final" == "n" ]; then
    read -p "Enter pre-release (rc, etc): " prerelease
    echo $version-$prerelease
    changie batch $version-$prerelease --keep
    if [ ! -d /.changes/$version ]; then
        echo $version
        echo "$(pwd)"
        mkdir ./.changes/$version
    fi
    mv ./.changes/unreleased/* ./.changes/$version/
else
    # TODO: remove all prerelease lines from current changelog
    mv ./.changes/$version/* ./.changes/unreleased/
    changie batch $version
fi

python ./scripts/add-contributors.py $version

changie merge
