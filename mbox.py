"""
python mbox.py

https://www.loc.gov/preservation/digital/formats/fdd/fdd000383.shtml#:~:text=MBOX%20(sometimes%20known%20as%20Berkeley,the%20end%20of%20the%20file.


"""
# ******************************************************************************************************************120

# standard imports
import mailbox

# 3rd party imports
import click

# custom imports

@click.command()
@click.option('-f', '--filename', type=str, required=True, help='Input File Path')
def main(filename: str) -> None:
    """Main Function"""
    counter = 0

    m = mailbox.mbox(filename)
    print(len(m))


    # for message in mailbox.mbox(filename):
    #     print(message['subject'])
    #     counter = counter + 1
    #     if counter > 15:
    #         break

if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
