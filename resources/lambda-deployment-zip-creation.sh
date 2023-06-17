# pip install --platform manylinux2014_x86_64 --target=<folder_name> --implementation cp --python 3.9 --only-binary=:all: --upgrade <lib1> <lib2>


pip install --platform manylinux2014_x86_64 --target lambda-function-code --implementation cp --python 3.10 --only-binary :all: --upgrade pymongo boto3 requests