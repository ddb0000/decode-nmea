from pyais import decode
from pyais.stream import FileReaderStream

# Example AIS message (replace with actual lines from your file)
ais_sentence = "!AIVDM,1,1,,B,1815UdP000Ld<0aj<F76cC5f0@Q6,0*49,2024-09-11 00:00:01"

msg = decode(ais_sentence)
print(msg)  # Parsed AIS message dictionary
