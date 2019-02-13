# s3-kompressor
Tool to create compressed backups of s3 buckets

## Current state
- can download s3 contents to local hard disk and slice it into zip files
- upload the slices back to s3
- adding index files on the side of the zips so you'd know what is where

## Does it work ?
 - if backing up 4 terabytes of data in 20+ million files counts as "it works" then yes it does work.
