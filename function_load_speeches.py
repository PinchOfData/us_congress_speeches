def printwrap(text, width=150):
    """
    Wraps the input text to a specified width and prints it.
    
    Parameters:
    text (str): The input text to be wrapped and printed.
    width (int, optional): The maximum line width for wrapping the text. 
                           Default is 150 characters.
    """
    print(textwrap.fill(text, width=width))


def clean_text(text):
    """
    Cleans and formats the input text by applying various regex substitutions.

    This function processes the input text to remove unnecessary characters,
    whitespace, and specific patterns, resulting in a cleaner and more readable
    string. The cleaning steps include removing speaker breaks, collapsing
    multiple spaces into a single space, replacing quotation marks with standard
    characters, and eliminating specific phrases or patterns that are deemed
    irrelevant for the desired output.

    Parameters:
    text (str): The input text to be cleaned and formatted.

    Returns:
    str: The cleaned and formatted text after applying the regex substitutions.
    """
    text = re.sub(r'\nf\s\n', ' <SPEAKER_BREAK> ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'(?<![.!?])\n', ' ', text)
    text = re.sub(r'(\w+)-\s+(\w+)', r'\1\2', text)
    text = re.sub(r'[“”]', '"', text)
    text = re.sub(r'[‘’]', "'", text)
    text = re.sub(r'–|—', '-', text)
    text = re.sub(r'…', '...', text)
    text = re.sub(r'Jkt \d{6} PO \d{5} Frm \d{5} Fmt \d{4} Sfmt \d{4}', '', text)
    text = re.sub(r'E:\\CR\\FM\\[A-Z0-9.]+ [A-Z0-9]+', '', text)
    text = re.sub(r'DMWilson on DSKJM0X7X2PROD with', '', text)
    text = re.sub(r'VerDate \w+ \d{2} \d{4} \d{2}:\d{2} \w+ \d{2}, \d{4}', '', text)
    text = re.sub(r'\b(CONGRESSIONAL|RECORD|HOUSE|DAILY|DIGEST)\b', '', text)
    text = re.sub(r'Pdnted on recycled papfil', '', text)

    return text


def extract_speeches_from_content(cleaned_text):
    """
    Extracts speaker names and their corresponding speeches from the cleaned text.

    This function uses a regular expression to identify and extract speech segments
    from the input text. It looks for matches that include speaker names and their
    respective speeches, and it formats the extracted data into a list of dictionaries,
    where each dictionary contains the speaker's name and the corresponding speech.

    Parameters:
    cleaned_text (str): The input text from which speeches will be extracted. 
                        This text should be cleaned and formatted appropriately.

    Returns:
    list of dict: A list of dictionaries, each containing:
        - "Speaker" (str): The name of the speaker.
        - "Speech" (str): The corresponding speech of the speaker.
    """
    matches = re.findall(pattern, cleaned_text, re.DOTALL)

    results = []
    for match in matches:
        speaker = (match[1] + " " + match[2]).strip()
        speech = match[3].strip()
        results.append({
            "Speaker": speaker,
            "Speech": speech
        })

    return results

