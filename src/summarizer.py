from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import os
from typing import List
from collections import defaultdict
import torch

from unlimiformer import Unlimiformer
from usage import UnlimiformerArguments

SUMMARY_MODEL_PATH = os.environ['SUMMARY_MODEL_PATH']

class Summarizer():
    def __init__(self):
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.tokenizer = AutoTokenizer.from_pretrained(SUMMARY_MODEL_PATH)
        
        defaults = UnlimiformerArguments()
        unlimiformer_kwargs = {
            'layer_begin': defaults.layer_begin, 
            'layer_end': defaults.layer_end,
            'unlimiformer_head_num': defaults.unlimiformer_head_num, 
            'exclude_attention': defaults.unlimiformer_exclude, 
            'chunk_overlap': defaults.unlimiformer_chunk_overlap,
            'model_encoder_max_len': defaults.unlimiformer_chunk_size,
            'verbose': defaults.unlimiformer_verbose, 'tokenizer': self.tokenizer,
            'unlimiformer_training': defaults.unlimiformer_training,
            'use_datastore': defaults.use_datastore,
            'flat_index': defaults.flat_index,
            'test_datastore': defaults.test_datastore,
            'reconstruct_embeddings': defaults.reconstruct_embeddings,
            'gpu_datastore': defaults.gpu_datastore,
            'gpu_index': defaults.gpu_index
        }
        model = AutoModelForSeq2SeqLM.from_pretrained(SUMMARY_MODEL_PATH).to(self.device)
        self.model = Unlimiformer.convert_model(model, **unlimiformer_kwargs)
        self.model.eval()
        self.model.to(self.device)
        
    def resolve_multiple_mentions(self, document:str, targets: List):
        targets_dict = defaultdict(list)

        # Group targets by UUID
        for target in targets:
            targets_dict[target.target_uuid].append(target)
        
        # Get most common/longest mention for every UUID
        for uuid, targets in targets_dict.items():
            mentions = [document[target.span_start:target.span_end].strip() for target in targets]
            # Sort list by length to return the longest mention if tie
            mentions.sort(key=len, reverse=True)
            # Get most common mention
            targets_dict[uuid] = max(mentions, key=mentions.count) 
        
        return targets_dict


    def generate_prompt(self, entity_list:List , entity:str, document:str):
        entity_list.remove(entity)
        other_entities = "\n".join([f"[{idx+2}] {ent.strip()}" for idx, ent in enumerate(entity_list)])
        prompt = f"[1] {entity.strip()}\n{other_entities}\ndocument: {document}"
        return prompt

    def summarize(self, document: str, targets: List, ):
        torch.cuda.empty_cache()
        response = []
        targets_dict = self.resolve_multiple_mentions(document, targets)
        mentions = [mention for mention in targets_dict.values()]
        for target_uuid, target_mention in targets_dict.items():
            print(f"Target UUID: {target_uuid}, Mention: {target_mention}")
            prompt = self.generate_prompt(mentions.copy(), target_mention, document)
            # inputs = self.tokenizer(prompt, return_tensors="pt", max_length=1024, truncation=True).to(self.device)
            inputs = self.tokenizer(prompt, return_tensors="pt", truncation=False).to(self.device)
            self.model.config.eos_token_id=50118 # sets newline ("\n") token as EOS token to terminate generation
            summary_ids = self.model.generate(**inputs, force_words_ids=self.tokenizer([f"[1] {target_mention}:"], add_special_tokens=False).input_ids, max_new_tokens=1024, num_beams=4, do_sample=False, encoder_repetition_penalty=1.5) # penalise if output token not seen in enconder input
            summary = self.tokenizer.batch_decode(summary_ids, skip_special_tokens=True, clean_up_tokenization_spaces=False)[0]
            try:
                ent, summ = summary.replace(" [", "\n[").split("\n")[0].replace("[1] ", "").split(":")
                response.append({"target_uuid": target_uuid, "summary": summ.strip()}) 
            except Exception as e:
                print(summary)
        return response