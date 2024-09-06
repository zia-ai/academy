"""
python template.py

"""
# ******************************************************************************************************************120

# standard imports
import json

# 3rd party imports
import click
import pandas

# custom imports

CONTEXT_IDS=["972","5259","396","2001","3037","3146","894","4162","4239","1408","2804","4718",
             "1582","2743","2862","859","5024","5298","151","649","543","3926","4075","890","1750",
             "5414","264","4631","3721","3630","3010","1724","3064","4719","4070","2224","2675",
             "3487","734","3698","413","5333","1750","3373","611","4706","4892","413","694",
             "3130","4054","5466","479","2527","2920","2972","2033","4718","2249","5532",
             "3201","3931","2356","1635","3493","1983","1285","2425","5156","4563","1693",
             "1334","656","1219","217","3627","3079","1674","5357","4048","4066","3145","2918","4409",
             "3161","3088","2194","1642","2298","4268","2665","4781","1979","972","5313","2976","689",
             "4739","4647","2669","3488","129","4211","4847","5409","2029","481","5167","3052","2996",
             "5199","4265","587","2997","2570","229","3344","5268","4200","4569","3135","1035","1848",
             "3537","3099","2266","1978","636","2376","815","2869","3352","3117","3593","185","4215",
             "840","564","2855","1914","1172","3294","533","1491","2670","2456","436","2126","3339",
             "3166","308","617","4444","2064","4899","2975","4368","1791","3120","1452","4526","4772","1919","4343","4072","2025"]

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None: # pylint: disable=unused-argument
    """Main Function"""
    file_in = open(filename,mode="r",encoding="utf8")
    json_in = json.load(file_in)
    file_in.close()
    df = pandas.json_normalize(json_in["examples"])
    df = df[df["context.context_id"].isin(CONTEXT_IDS)]
    
    # rename meta
    cols = df.columns.to_list()
    for c in cols:
        assert isinstance(c,str)
        if c.startswith("metadata."):
            df = df.rename(columns={c:c.replace("metadata.","")})
    cols = df.columns.to_list()

    # drops
    for c in cols:
        if c.startswith("context.") or c in [
            "tags","intents","id","conversation_turn","first_customer_utt","second_customer_utt","last_customer_utt"
        ]:
            df.drop(columns=[c],inplace=True)
    cols = df.columns.to_list()
    print(cols)

    output_filename = filename.replace(".json","_output.csv")
    df.to_csv(output_filename,index=False,header=True)
    print(f'Wrote to: {output_filename}')
    

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
