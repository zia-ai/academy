# Mbox analysis overview

These scripts are written for the situation where you have a complete download of a mbox file.
Particularly a gmail mbox file.
This is analagous to if you want a complete visualisation across a mailfile using PxD, or
if you are looking for concepts within an email file.  For instance a complicance audit.
If you have interactive access to the mail file you may want to do an interactive query
and instead upload a smaller subset.

The assumption is that there is a very large mailfile of potentially unknown providence that needs as 
complete as possible processing.

These scripts are written to:
 - work with very large mail files 50GB+
 - support restartability
 - deal with corruptions

To do this they avoid the typical mailbox processing libraries and go to first priniciple mailbox text processing.  The text information interested in, will be buried along side lots of attachments which are
encoded for mail transmission.  The mailbox is read sequentially, finding each email start, then dealing with multipart mime sections, ignoring non-text ones (videos, images, pdfs etc) and then writing out in a JSON format a single document for each email in a year/month folder structure.

As such it is disk IO intensive, but memory light.

## To download a test file.

Go to your gmail account takeout : https://takeout.google.com/settings/takeout

Unselect all
Go down the list and only select Mail
Click on Next Step
Click Export Once
This will process and you'll receive a mbox file by email, it will be a large file. 

## split the large file into a folder of emails

All filenmaes must be fully qualified with their directory i.e
./data/mbox/mymbox or 
/home/ubuntu/mymbox.mbx

Run ```python mbox_splitter.py --filename <mbox_filename>``` 

Other options can be used to do dummy runs, adjust reporting frequence etc but are not required.
This will take a while to run but give progress updates.
Ensuring it is running on a fast drive will increase speed.

It will create a folder in the same directory as the file named (output) or any provided --run_name 
within that will be a folder per year, having in it a folder per month, within that a json
per email.  Years and months only created where emails exist.

## manage token size and enrich the json with additional information useful for analytics

Run ```python mbox_add_pp.py --directory <input_dir> --output_directory <output_dir>``` 

This runs through the directory created by the splitter.
It creates a mirror directory with the name you provide.
This will reduce the token size of the emails buy removing email links.
It then provides a metadata field which includes just the raw email part of any email
In any part of To/From/CC or mentioned in the text.
I.e it provides in HF the ability to do a "Contains" match on an email and find every message 
that email is involved in any way.
Then it writes that to the mirror.

## turn into a CSV for upload to humanfirst.

Run ```python mbox_make_csv.py --directory <input_dir>```

Loops through the directory created by mbox_add_pp and creates a csv of emails ready to either
- upload in HF studio with the custom CSV loaded
- produce a JSON to upload 

## upload to humanfirst with CSV custom upload

Use the gui or go on to JSON if very large.

## produce a JSON file

Use ```csv_to_jso_unlabelled.py``` to produce a JSON file which you can then gzip and upload

## Appendix

### Analytics
```mbox_analytics``` probably not of interest - produced some custom analytics.




