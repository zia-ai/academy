"""
python ./archive/tennis.py

"""
# ******************************************************************************************************************120

# Third party imports
import openai
import numpy
import click
import openai.openai_object

BASELINE_ARTICLE = """
Wimbledon Prediction

Carlos Alcaraz defeated Novak Djokovic in the 2023 Wimbledon final, putting a remarkable end to the Serbian's four straight titles at the All England Club.
Djokovic would be considered Alcaraz's biggest threat, but it is unknown if he will be ready to play after recently tearing his meniscus.
Alcaraz will still face hard competition, though.
Jannik Sinner has made it to at least the quarterfinals of Wimbledon in back-to-back years.
On top of this, he is arguably the most in-form player on the ATP tour at the moment.
Elsewhere, Daniil Medvedev is coming off the heels of his first Wimbledon semifinal appearance, hungry to avenge his defeat to Alcaraz.
Another potential challenger for Alcaraz is Alexander Zverev, who, despite his limited success at Wimbledon, equips the tools to make all opponents wary.
Despite the challenges he'll face, Alcaraz should still be able to make it to the final.
Prediction: Alcaraz advances to championship match.

"""

OUTPUT_ARTICLE = """
### Wimbledon Final Clash: Alcaraz vs. Sinner—Who Will Triumph?
In an eagerly anticipated showdown at Wimbledon, tennis fans around the globe are set to witness a riveting contest between two of the sport's brightest stars: Carlos Alcaraz and Jannik Sinner. Both athletes, renowned for their fiery competitiveness and exceptional skill sets, will vie for supremacy on the grass courts. Alcaraz, a prodigious talent from Spain, and Sinner, Italy's trailblazing sensation, each bring a dynamism and youthful vigor that promises an exhilarating final.
Carlos Alcaraz has taken the tennis world by storm with his rapid rise to prominence. Turning professional in 2018, his ascent has been underscored by a remarkable ability to conquer seasoned opponents. At just 19 years of age, Alcaraz became the youngest man to achieve the world number one ranking, a testament to his prodigious talent and unyielding work ethic. His arsenal is replete with powerful forehands, an exceptionally effective drop shot, and a robust net game. These attributes were instrumental in his victories at the 2022 US Open, 2023 Wimbledon, and most recently, the 2024 French Open, establishing him as a formidable contender on various surfaces.
Conversely, Jannik Sinner’s journey has been one of meticulous progression and relentless determination. Having cultivated his skills at a young age with the tutelage of veteran coach Riccardo Piatti, Sinner's development has been punctuated by notable milestones, including his historic rise to world number one in 2024. His laser-focused backhand and aggressive baseline play, complemented by a powerful serve, have consistently troubled not only his peers but also experienced luminaries such as Novak Djokovic. Sinner's resilience was remarkably showcased when he clinched the 2024 Australian Open title, overcoming both Djokovic and Daniil Medvedev.
When analyzing their head-to-head encounters, Alcaraz holds a narrow lead over Sinner with a 5-4 record. Crucially, Alcaraz emerged victorious in their previous grand slam clashes, including a memorable five-set thriller at the 2022 US Open. However, this year, Sinner has showcased commendable improvement, exemplified by his semi-final victory against Alcaraz at the 2024 French Open, a match that not only tested their physical limits but also their mental fortitude.
On the grass courts of Wimbledon, Sinner may have a slight edge considering his 2023 semifinal appearance and his noticeable comfort on this surface. Sinner’s game is particularly well-suited to fast courts, where his penetrating groundstrokes and swift movement can wreak havoc. His recent form, capturing two significant titles including his Masters 1000 win in Miami and the Grand Slam success in Australia, further bolsters his credentials as a top contender.
However, Alcaraz’s adaptability and tenacity cannot be understated. His victory over Djokovic on grass at the 2023 Wimbledon final is a testament to his burgeoning mastery of the surface. Alcaraz's impressive net play, combined with his ability to maneuver opponents with strategic drop shots and relentless baseline aggression, renders him a significant threat. His recent triumph at Indian Wells and his consistent performance throughout the hard and clay seasons demonstrate a versatility that could prove pivotal.
In conclusion, predicting the outcome of this titanic Wimbledon final is a formidable challenge. Both Alcaraz and Sinner have compiled records worthy of future hall-of-famers and possess a suite of skills that can disrupt the other. However, considering Sinner's recent large-scale victories and his growing prowess on grass, the slight advantage may lean towards him. Nevertheless, tennis, rich with unpredictability, ensures that this match will be a testament to their extraordinary talents, promising nothing short of high drama and exceptional shot-making at the hallowed grounds of Wimbledon.
"""


@click.command()
@click.option('-a', '--api_key', type=str, required=True, help='API Key')
def main(api_key: str) -> None: # pylint: disable=unused-argument
    """Main Function"""

    openai.api_key = api_key

    embed_baseline = get_embedding(BASELINE_ARTICLE)
    embed_output = get_embedding(OUTPUT_ARTICLE)
    print(cosine_similarity(embed_baseline,embed_output))

def get_embedding(text,model="text-embedding-3-large"):
    """Get embeddings"""
    text = text.replace("\n", " ")
    return openai.Embedding.create(input = [text], model=model).data[0].embedding

def cosine_similarity(a, b):
    """Get Cosine Similarity"""
    return numpy.dot(a, b) / (numpy.linalg.norm(a) * numpy.linalg.norm(b))

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
