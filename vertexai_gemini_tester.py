"""
python vertex_gemini_tester.py

Runs a test call against claude sonnet v2 on vertex ai providing custom metdata
to test whether billing can be differentiated.

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import vertexai
import vertexai.generative_models

# custom imports

@click.command()
@click.option('-i', '--project_id', type=str, required=True, help="Project id for google vertexai")
@click.option('-p', '--prompt', type=str, required=False, default="Tell me a joke, try to come up with something different not just about scientists", 
              help='Prompt text as a string')
@click.option('-m', '--model_id', type=str, required=False, default = "gemini-1.5-pro", help="Vertex AI model ID")
@click.option('-r', '--region_id', type=str, required=False, default = "us-east5", help="Google Cloud Region ID")
def main(project_id: str,
         model_id: str,
         prompt: str,
         region_id) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    vertexai.init(project=project_id, location=region_id)
    model = vertexai.generative_models.GenerativeModel(model_id,) # can set default lables here
    
    labels = {
        "system": "hf",
        "namespace": "python",
        "playbook_id": "playbook-example"
    }
    
    generation_config = {
        "temperature": 2.0,
    }


    response = model.generate_content(labels=labels,
                                      contents=prompt,
                                      generation_config=generation_config
    )

    print(response.text)

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
