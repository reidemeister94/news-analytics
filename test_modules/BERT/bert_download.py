from transformers import TFBertModel, BertTokenizer

tokenizer = BertTokenizer.from_pretrained("bert-base-multilingual-cased")
model = TFBertModel.from_pretrained("bert-base-multilingual-cased")
