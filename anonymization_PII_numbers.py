#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ***************************************************************************80
#
# python anonymization_PII_numbers.py
#
# *****************************************************************************

# standard imports
import re
import random

# 3rd party imports
import click
import pandas

@click.command()
@click.option("-i","--input",required=True, help="input text file containing utterances")
@click.option("-r","--replace_with_0",is_flag=True,default=False,help="replace the digits with 0")
def main(input: str, replace_with_0: bool) -> None:
    """Main Function"""

    with open(input, mode="r", encoding="utf8") as f:
        utterances = f.read()

    utterances = utterances.split("\n")

    df = pandas.DataFrame(data=utterances,columns=["utterances"])

    print(f"Size of dataframe before processing: {df.shape}")

    df["anonymized_phrases"] = ""

    re_anydigit = re.compile(r"[0-9]+")
    df["utterance_with_digits"] = df["utterances"].apply(lambda x: True if re.findall(re_anydigit,x) else False)

    re_more_than_16_digit = re.compile(r"(?:\d[- ]?){16,}\d")
    df["utterance_more_than_16_digit"] = df["utterances"].apply(lambda x: True if re.findall(re_more_than_16_digit,x) else False)

    re_16_digit = re.compile(r"(?:\d[- ]?){15}\d")
    df["utterance_16_digit"] = df["utterances"].apply(lambda x: True if re.findall(re_16_digit,x) else False)

    re_more_than_4_digit = re.compile(r"(?:\d[- ]?){5,}\d")
    df["utterance_with_more_than_4_digit"] = df["utterances"].apply(lambda x: True if re.findall(re_more_than_4_digit,x) else False)

    re_cost = re.compile(r"\$[0-9,\.]{1,11}")
    df["utterance_with_cost"] = df["utterances"].apply(lambda x: True if re.findall(re_cost,x) else False)

    re_ordinal = re.compile(r"([0-9]{1,2})(nd|th|rd)")
    df["utterance_with_ordinal"] = df["utterances"].apply(lambda x: True if re.findall(re_ordinal,x) else False)

    re_4_digit = re.compile(r"(?:\d[- ]?){3}\d")
    df["utterance_with_4_digit_between_1970_and_2030"] = df["utterances"].apply(lambda x: True if set(re.findall(re_4_digit,x)).intersection(set([str(i) for i in range(1970,2031)])) else False)

    df["utterance_with_4_digit"] = df["utterances"].apply(lambda x: True if re.findall(re_4_digit,x) else False)

    re_threedigit = re.compile(r"(?:\d[- ]?){2}\d")
    df["utterance_with_three_digit_and_cvv"] = df["utterances"].apply(lambda x: True if ((x.lower().find("cvv") != -1) and re.findall(re_threedigit,x)) else False)

    df["utterance_with_three_digit"] = df["utterances"].apply(lambda x: True if re.findall(re_threedigit,x) else False)

    re_twodigit = re.compile(r"([0-9]{2})")
    df["utterance_with_two_digits"] = df["utterances"].apply(lambda x: True if re.findall(re_twodigit,x) else False)
    
    # re_singledigit = re.compile(r"\b[0-9]\b")
    re_singledigit = re.compile(r"[0-9]")
    df["utterance_with_single_digits"] = df["utterances"].apply(lambda x: True if re.findall(re_singledigit,x) else False)
    
    classes = df.columns.tolist()
    classes.remove("utterances")
    classes.remove("utterance_with_digits")
    classes.remove("anonymized_phrases")

    df["class"] = df.apply(assign_class,args=[classes],axis=1)
    print(df[["utterances","class"]].groupby(["class"]).count())

    df["anonymized_phrases"] = df["utterances"]

    df.loc[df["class"]=="utterance_more_than_16_digit","anonymized_phrases"] = df.loc[df["class"]=="utterance_more_than_16_digit","utterances"].apply(replacement,args=[re_more_than_16_digit, replace_with_0])

    df.loc[df["class"]=="utterance_16_digit","anonymized_phrases"] = df.loc[df["class"]=="utterance_16_digit","utterances"].apply(replacement,args=[re_16_digit, replace_with_0])

    df.loc[df["class"]=="utterance_with_more_than_4_digit","anonymized_phrases"] = df.loc[df["class"]=="utterance_with_more_than_4_digit","utterances"].apply(replacement,args=[re_more_than_4_digit, replace_with_0])

    output_csv = input.replace(".txt",".csv")
    df.to_csv(output_csv,sep=",",encoding="utf",index=False)
    print(f"\nSize of dataframe after processing: {df.shape}")
    print(f"Processed dataset is stored as CSV in {output_csv}")

    output_anonymized = input.replace("_cleansed.txt","_anonymized.txt")
    anonymized_phrases = df["anonymized_phrases"].to_list()
    anonymized_phrases = "\n".join(anonymized_phrases)
    with open(output_anonymized, mode="w", encoding="utf8") as f:
        f.write(anonymized_phrases)
    print(f"Anonymized phrases are stored in {output_anonymized}")
    

def replacement(utterance: str,pattern: re, replace_with_0: bool) -> str:
    """performs random shuffling of numbers"""

    re_digits = re.compile(r"[0-9]+")

    matches = pattern.finditer(utterance)
    for match in matches:
        span = match.span()
        needs_shuffling = utterance[span[0]:span[1]]

        digits = re.findall(re_digits,needs_shuffling)

        if replace_with_0:
            for i,digit in enumerate(digits):
                digits[i] = re.sub(r"[0-9]","0",digits[i])
            
            shuffled = " ".join(digits)
        else:
            for i,digit in enumerate(digits):
                digits[i] = ''.join(random.sample(digits[i], len(digits[i])))
            
            shuffled = " ".join(random.sample(digits,len(digits)))
        utterance = utterance.replace(needs_shuffling,shuffled)

    return utterance

def assign_class(row:pandas.Series, classes) -> str:
    """Assigns class to the utterance"""
    
    if row["utterance_with_digits"] == False:
        return "no_digits"
    
    for c in classes:
        if row[c]:
            return c
        
    return "None"

if __name__=="__main__":
    main()

