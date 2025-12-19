# test_compare.py
from collections import Counter
import os
import re
import unicodedata
from loaders import jsonDataSource  # твой загрузчик jsonl

def normalize_token(tok: str) -> str:
    # нормализуем unicode и приводим к нижнему регистру
    tok = unicodedata.normalize('NFKC', tok)
    return tok.lower()

def words_from_text_file(path):
    # извлекает слова (Unicode-слова), возвращает генератор слов
    pattern = re.compile(r"\w+", flags=re.UNICODE)
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            for m in pattern.findall(line):
                yield normalize_token(m)

def load_mr_results(dirpath):
    # собирает финальные reduce-результаты (json/ jsonl) в один dict
    mr = {}
    for fname in os.listdir(dirpath):
        path = os.path.join(dirpath, fname)
        # предполагаем, что файл в формате jsonl: каждая строка — json объект {word: count}
        for rec in jsonDataSource.load(path):
            for k, v in rec.items():
                mr[k] = mr.get(k, 0) + int(v)
    return mr

if __name__ == "__main__":
    # 1) эталонный подсчёт по исходному тексту
    source_text = r'large_test_words.txt'   # поставь свой путь к исходному тексту
    ground_counter = Counter(words_from_text_file(source_text))

    # 2) загрузка MR reduce output
    mr_dir = r"C:\ovr_pr\worker\reduce_output"   # путь где у тебя reduce output
    mr = load_mr_results(mr_dir)

    # 3) сравнение: какие слова отличаются
    all_keys = set(mr) | set(ground_counter)
    diffs = {}
    for k in all_keys:
        a = mr.get(k, 0)
        b = ground_counter.get(k, 0)
        if a != b:
            diffs[k] = (a, b)

    # 4) отчёт: сколько всего, сколько совпало, топ-10 отличий
    print("Всего ключей MR:", len(mr))
    print("Всего ключей эталон:", len(ground_counter))
    print("Найдено несовпадений:", len(diffs))
    if diffs:
        # отсортируем по величине абсолютной разницы
        top = sorted(diffs.items(), key=lambda kv: abs(kv[1][0]-kv[1][1]), reverse=True)[:30]
        print("Топ-30 отличников (mr_count, ground_count):")
        for k, (mcount, gcount) in top:
            print(f"{k!r}: MR={mcount}, expected={gcount}")
    else:
        print("Совпадает полностью. Неплохо.")
