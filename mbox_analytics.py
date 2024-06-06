"""
python mbox_analytics.py

Pickup the CSV and do some analytics

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas
import plotly.express as px

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Filename CSV')
@click.option('-h', '--home_org', type=str, required=True, help='Home org to drop')
@click.option('-o', '--more_than_n', type=int, required=False, default=10,
              help='Ignore any email only done once')
def main(filename: str,
         home_org:str,
         more_than_n: int) -> None: # pylint: disable=unused-argument
    """Main Function"""

    columns_care_about = ['participants','filename']
    df = pandas.read_csv(
        filename,
        usecols=columns_care_about,
        dtype={'participants':str,'filename':str}
    )
    print(df)

    df["participants_list"] = df["participants"].apply(get_str_list)
    df = df.explode(['participants_list'],ignore_index=False).reset_index(drop=True)
    df["participants_list"] = df["participants_list"].apply(cleanse)
    df["participants_list"] = df["participants_list"].apply(filter_junk)

    df["domain"] = df["participants_list"].apply(get_org)


    print(df.shape)
    df= df[~(df["domain"]==home_org)]
    df= df[~(df["domain"]=="junk_org")]
    print(df.shape)

    # Parent group

    gb = df[["filename","domain"]].groupby(['domain'],as_index=False).count()
    gb = gb.sort_values('filename',ascending=False)
    print(gb)
    top_500 = gb["domain"].to_list()[0:500]
    df.loc[~(df["domain"].isin(top_500)),"domain"] = "other"
    gb = df[["filename","domain"]].groupby(['domain'],as_index=False).count()
    gb = gb.sort_values('filename',ascending=False)

    fig = do_chart(fields = ["domain"],values= "filename",gb=gb)
    print(gb)

    # write csv
    output_csv = filename.replace(".csv","_domain_summary.csv")
    assert output_csv != filename
    gb.to_csv(output_csv,index=False)
    print(f"Wrote to {output_csv}")

    output_filename=filename.replace(".csv","_summary_output.html")
    assert filename != output_filename
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

    gb = df[["filename","domain","participants_list"]].groupby(['domain','participants_list'],as_index=False).count()
    if more_than_n > 0:
        gb = gb[~(gb["filename"] <= more_than_n)]
    gb = gb.sort_values('filename',ascending=False)

    fig = do_chart(fields = ["domain","participants_list"],values= "filename",gb=gb)
    print(gb)

    # write csv
    output_csv = filename.replace(".csv","_summary.csv")
    assert output_csv != filename
    gb.to_csv(output_csv,index=False)
    print(f"Wrote to {output_csv}")

    output_filename=filename.replace(".csv","_output.html")
    assert filename != output_filename
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

def do_chart(fields: list, values: str, gb: pandas.DataFrame):

    fig = px.treemap(gb, path=[px.Constant("<br>")] + fields, values=values)

    # format main body of treemap and add labels
    # colours set using template
    fig.update_traces(marker={"cornerradius":3})
    fig.update_layout(template='plotly', width=1500, height=750)
    fig.update_traces(textinfo="label + percent root")
    fig.update_traces(root_color="#343D54")


    # set the label font and size
    fig.data[0]['textfont']['size'] = 12
    fig.data[0]['textfont']['family'] = 'Calibri'

    #format title, hover info fonts and background
    fig.update_layout(
    title= "Overview of categories by total count",
    title_y=0.98,
    title_font_color = 'white',
    title_font_size = 24,
    hoverlabel=dict(
            font_size=16,
            font_family="Calibri"
        ),
    paper_bgcolor="#343D54",
    )

    # Update the hover info
    fig.data[0].hovertemplate = (
    '<b>%{label}</b>'
        '<br>' +
    'Count: %{value}' +
    '<br>' +
    'Percent of all utterances: <i>%{percentRoot:.1%} </i>'+
    '<br>' +
    'Percent of all parent category: <i>%{percentParent:.1%} </i>'+
    '<br>'
    )
    #change margin size - make the plot bigger within the frame
    fig.update_layout(margin = dict(t=38, l=10, r=10, b=15))

    return fig

def cleanse(text: str) -> str:
    return text.strip('\'')

def filter_junk(text: str) -> str:
    if ".gif@" in text or ".jpg@" in text or ".png@" in text:
        return "image_email"
    else:
        return text


def get_org(text:str) -> str:
    if text == "image_email":
        return "junk_org"
    return text.split("@")[-1]

def get_str_list(text:str) -> list:
    text = str(text)
    text = text[1:-1]
    text = text.replace(", ",",")
    text = text.replace("|","")
    return text.split(",")


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
