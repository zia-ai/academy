#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
#  python codepage.py
# 
#  Example of how to fix code page errors if you have opened your 
#  utf 8 CSV in XSLX using a double click by mistake
#
# *****************************************************************************

# third party imports
import click

@click.command()
def main():
    text = 'Avant dâ€™annuler votre abonnement, Ã  quel point Ã©tiez-vous satisfait de vos services mobiles?'
    print(fixit(text))

def fixit(text):
    try:
        text = bytes(text,encoding='cp1252').decode('utf8')    
    except UnicodeEncodeError: 
        print(f'Couldn\'t re-encode: {text}')
    return text

if __name__ == '__main__':
    main()

