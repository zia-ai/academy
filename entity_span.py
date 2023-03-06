#! #!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# entity_span.py -t <text> -e <entity>
#
# *****************************************************************************

# third party imports
import click

@click.command()
@click.option('-t', '--text', type=str, required=True, help='text')
@click.option('-e', '--entity', type=str, required=True, help='entity')

def main(text: str, entity: str):
    """Main Function"""

    total_number_of_bytes, start_pos, end_pos = process(text,entity)
    print(f"total number of bytes of given text: {total_number_of_bytes}")
    print(f"start_pos (UTF8 byte OFFSET): {start_pos}")
    print(f"end_pos (UTF8 byte OFFSET): {end_pos}")

def process(text: str, entity:str) -> tuple:
    """Finds totoal number of bytes of given text, start and end position as UTF8 byte offset"""

    total_number_of_bytes = len(bytes(text,"utf8"))
    index = text.find(entity)
    if index != -1:
        if index == 0:
            start_pos = index
        else:
            start_pos = len(bytes(text[0:index],"utf8"))
        end_pos = start_pos + len(bytes(entity,"utf8"))
    else:
        print("Given entity is not present in the text")
        quit()
    return total_number_of_bytes, start_pos, end_pos

if __name__=="__main__":
    main()
