from transformers import pipeline

# Allocate a pipeline for sentiment-analysis
nlp = pipeline("sentiment-analysis")
res = nlp("This is a sentence where i say that i'm happy")
res_2 = nlp("This is a sentence where i say that i'm sad")
print(res, res_2)
