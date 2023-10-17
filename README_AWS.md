How to mount S3

Install AWS

    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    sudo apt-get install unzip
    unzip awscliv2.zip
    sudo ./aws/install

Authenticate
get credentials from 1Password and save as .env
export $(cat .env | xargs)
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html


https://docs.aws.amazon.com/cli/latest/userguide/cli-services-s3-commands.html


    aws s3 ls

    aws s3 cp <path> ./data

