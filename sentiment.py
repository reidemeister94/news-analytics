from transformers import pipeline

# Allocate a pipeline for sentiment-analysis
nlp = pipeline('sentiment-analysis')
res = nlp("""Bought for my boyfriend. The 4TB external worked great for his PS4. He said some of the memory was used for converting but he has 7x the amount of space he started with. He is very happy with this item and I will update my review if he comes across any problems.""")
print(res)
