import boto3
from tqdm import tqdm
from botocore.exceptions import ClientError

from util import ETLConfig


class S3Loader:

    def __init__(self, aws_profile: str, bucket_name: str, config: ETLConfig):
        self.bucket_name = bucket_name
        self.config = config
        self.s3_client = boto3.session.Session(profile_name=aws_profile).client('s3')

    def _upload_file(self, local_path: str, s3_key: str):
        """Upload a single file to S3 with progress bar, only if it doesn't exist"""
        try:
            try:
                self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    pass
                else:
                    print(f"\nError checking existence of {s3_key}: {e}")
                    return False

            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
            )
            return True
        except Exception as e:
            print(f"\nError uploading {local_path}: {e}")
            return False
    
    def load_all(self):
        """Upload all files from local folder to S3"""
        print(f'Loading all transformed files from {self.config.transformation_folder}')
        
        if not self.config.transformation_folder.exists():
            print(f"Error: Local folder '{self.config.transformation_folder}' does not exist")
            return
        
        files_to_upload = [f for f in self.config.transformation_folder.rglob('*') if f.is_file()]
        
        uploaded_count = 0
        failed_count = 0
        
        print(f"Found {len(files_to_upload)} files to upload")
        print(f"Uploading to s3://{self.bucket_name}/{self.config.s3_folder}\n")
        
        for file_path in tqdm(files_to_upload, desc='Loading:'):
            relative_path = file_path.relative_to(self.config.transformation_folder)
            s3_key = self.config.s3_folder + str(relative_path).replace('\\', '/')
            
            if self._upload_file(str(file_path), s3_key):
                uploaded_count += 1
            else:
                failed_count += 1

            if failed_count > 10:
                print(f"Stopping after 10 failed uploads")
                return
        
        print(f"\nUpload complete!")
        print(f"Successfully uploaded: {uploaded_count} files")
        if failed_count > 0:
            print(f"Failed: {failed_count} files")
