import os
import shutil


def delete_directory(directory):
    """Delete a directory and all its contents."""
    shutil.rmtree(directory)


def create_directory(directory_path):
    # Check if the directory already exists
    if not(os.path.exists(directory_path)):
        # If it doesn't exist, create the directory
        os.makedirs(directory_path)

