"""
python heatmap.py -f <download_from_hf> -m <lookup for model>


"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import pandas
import plotly.express as px

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
@click.option('-m', '--model', type=str, required=True, help='Model names')
@click.option('-c', '--clip', type=float, required=False, default=0.35, help='Clip Point')
def main(filename: str, model: str, clip: float) -> None:
    """Main Function"""

    # read file
    df = pandas.read_csv(filename,delimiter=",",encoding="utf8")

    # get hierarchy
    df_model = pandas.read_csv(model,delimiter=",",encoding="utf8")
    df_model.set_index("child",drop=True,inplace=True)

    # work out column name
    for col in df.columns.to_list():
        if col.startswith("top_matching_intent_name"):
            top_matching_intent_name = col
            print(top_matching_intent_name)
            break

    # work out conf name
    for col in df.columns.to_list():
        if col.startswith("top_matching_intent_score"):
            top_matching_intent_score = col
            print(top_matching_intent_score)
            break

    # calc clips
    df = df.apply(apply_clip,args=[clip,top_matching_intent_name,top_matching_intent_score],axis=1)

    # apply parents
    df = df.join(df_model,on="child")

    # group it
    gb = df[["parent","child","id"]].groupby(["parent","child"]).count().reset_index()
    gb.rename(columns={"id":"id_count"},inplace=True)
    print(gb)

    # Create the treemap plot using Plotly - using px.Constant("<br>") makes a prettier hover info for the root level
    fig = px.treemap(gb, path=[px.Constant("<br>"), 'parent', 'child'], values='id_count')

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

    fig.show()
    output_filename=filename.replace(".csv","_output_treemap.html")
    assert filename != output_filename
    fig.write_html(output_filename)
    print(f'Wrote to: {output_filename}')

def apply_clip(row: pandas.Series, clip: float,
               top_matching_intent_name: str,
               top_matching_intent_score: str ) -> pandas.Series:
    """Apply clip"""
    if float(row[top_matching_intent_score]) >= float(clip):
        row["child"] = row[top_matching_intent_name]
        row["score"] = row[top_matching_intent_score]
    else:
        row["child"] = "other"
        row["score"] = row[top_matching_intent_score]
    return row

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
