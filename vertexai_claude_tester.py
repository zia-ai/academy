"""
python vertex_gemini_tester.py

Runs a test call against claude sonnet v2 on vertex ai providing custom metdata
to test whether billing can be differentiated.

"""
# ******************************************************************************************************************120

# standard imports

# 3rd party imports
import click
import anthropic


# custom imports

@click.command()
@click.option('-i', '--project_id', type=str, required=True, help="Project id for google vertexai")
@click.option('-p', '--prompt', type=str, required=False, default="Tell me a joke, try to come up with something different not just about scientists", 
              help='Prompt text as a string')
@click.option('-m', '--model_id', type=str, required=False, default = "claude-3-5-sonnet-v2@20241022", help="Vertex AI Claude model ID with @ section")
@click.option('-r', '--region_id', type=str, required=False, default = "us-east5", help="Google Cloud Region ID")
def main(project_id: str,
         model_id: str,
         prompt: str,
         region_id) -> None: # pylint: disable=unused-argument
    """Main Function"""
    
    client = anthropic.AnthropicVertex(project_id=project_id, region=region_id)
    
    labels = {
        "system": "hf",
        "namespace": "python",
        "playbook_id": "playbook-example"
    }
       
    message = client.messages.create(
        model=model_id,
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        temperature=1,
        metadata={"user_id":"hf-playbook-pipeline"}

    )
    


    # print(message.model_dump_json(indent=2))
    print(message.model_dump()["content"][0]["text"])

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
