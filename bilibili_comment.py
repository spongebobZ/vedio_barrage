import threading
import requests
import numpy
from xml.parsers.expat import ParserCreate


class MySaxHandler:
    def __init__(self):
        self.p_coment_list = []
        self.coment = ''

    def start_element(self, name, attrs):
        if name == 'd':
            self.coment = attrs['p'] + ','

    def end_element(self, name):
        if name == 'd':
            self.p_coment_list.append(self.coment)
            self.coment = ''

    def char_data(self, text):
        self.coment += text

    def get_result(self):
        r = self.p_coment_list
        return r


write_lock = threading.Lock()


def run_all(thread_num, start_offset, end_offset):
    cid_list = list(range(start_offset, end_offset))
    per_thread_tasks = len(cid_list) // thread_num
    thread_list = []
    for i in range(thread_num):
        t = i < thread_num - 1 and threading.Thread(target=split_task, args=(
            cid_list[i * per_thread_tasks:(i + 1) * per_thread_tasks],)) or threading.Thread(
            target=split_task, args=(cid_list[i * per_thread_tasks:],))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()


def split_task(cid_list):
    task_limit = 100
    split_nums = sum(divmod(len(cid_list), task_limit))
    for i in range(split_nums):
        if i < split_nums - 1:
            fetch_comments(cid_list[i * task_limit: (i + 1) * task_limit])
        else:
            fetch_comments(cid_list[i * task_limit:])


def run(total, thread_num):
    cid_list = numpy.random.randint(100000, 99999999, total)
    per_thread_tasks = total // thread_num
    thread_list = []
    for i in range(thread_num):
        t = i < thread_num - 1 and threading.Thread(target=fetch_comments, args=(
            cid_list[i * per_thread_tasks:(i + 1) * per_thread_tasks])) or threading.Thread(
            target=fetch_comments, args=(cid_list[i * per_thread_tasks:]))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()


def fetch_comments(cid_list):
    """
    :param num: the comments to fetch
    :param output_dir: the file to save comments
    :return:
    """
    output_path = 'bilibili_coment_dir'
    output_partitions = 10
    part_list_prefix = 'part_'
    for i in range(output_partitions):
        locals()[part_list_prefix + str(i)] = []
    handler = MySaxHandler()
    comment = []
    coment_list = []
    for i in cid_list:
        parser = ParserCreate()
        parser.StartElementHandler = handler.start_element
        parser.EndElementHandler = handler.end_element
        parser.CharacterDataHandler = handler.char_data
        try:
            comment_url = 'http://comment.bilibili.com/%s.xml' % i
            r = requests.get(comment_url)
        except requests.exceptions.ConnectionError:
            continue
        if r.status_code != 200:
            continue
        parser.Parse(r.content)
        coment_list.extend(handler.get_result())
        if len(coment_list) == output_partitions * 1000:
            for coment in coment_list:
                locals()[part_list_prefix + str(int(coment.split(',')[:1]) % output_partitions)].append(coment)
            for i in range(output_partitions):
                if len(locals()[part_list_prefix + str(i)]) < 1000:
                    continue
                write_lock.acquire()
                try:
                    with open(output_path + '/part_' + str(i) + '.csv', 'a', encoding='utf-8') as fa:
                        tuple(map(lambda c: fa.write(c + '\n'), locals()[part_list_prefix + str(i)]))
                        locals()[part_list_prefix + str(i)].clear()
                finally:
                    write_lock.release()


def comment_count(path):
    """
    :param path: coment file path
    :return: coment world count
    """
    word_list = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as fr:
        while True:
            lines = fr.readline(1000)
            if not lines:
                break
            words = [','.join(x.split(',')[8:]) for x in lines.split('\n') if x.count(',') > 7]
            word_list.extend(words)
    word_dict = dict()
    for w in word_list:
        word_dict[w] = w in word_dict and word_dict[w] + 1 or 1
    word_count = sorted(word_dict.items(), key=lambda d: d[1], reverse=True)
    return word_count


def transform_coment(basepath, resultpath):
    """
    :param basepath: base coment file path
    :param resultpath: transform result path
    :return:
    """
    with open(basepath, 'r', encoding='utf-8', errors='ignore') as fr:
        for line in fr:
            pass
        while True:
            lines = fr.readline(1000)
            if not lines:
                break
            coment_text_list = [','.join(x.split(',')[8:]) for x in lines.split('\n') if x.count(',') > 7]
            with open(resultpath, 'a', encoding='utf-8') as fa:
                tuple(map(lambda w: fa.write(w + '\n'), coment_text_list))


def search_similar_coment(path, keyword):
    """
    :param path: coment file path
    :param keyword: key word to get similar coment
    :return: similar coment
    """
    similar_coment_list = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as fr:
        while True:
            lines = fr.readline(1000)
            if not lines:
                break
            match_words_list = tuple(filter(lambda x: keyword in x, lines.split('\n')))
            tuple(map(lambda w: similar_coment_list.append(w), match_words_list))
    similar_coment_dict = dict()
    for w in similar_coment_list:
        similar_coment_dict[w] = w in similar_coment_dict and similar_coment_dict[w] + 1 or 1
    similar_coment_count = sorted(similar_coment_dict.items(), key=lambda d: d[1], reverse=True)
    return similar_coment_count


if __name__ == '__main__':
    output_path = 'bilibili_coment_dir'
    run_all(10, 1000000, 1010000)
    # run(10000, 50, output_path)
    # comment_count(output_dir)
    # format_coment_path = 'bilibili_coment_dir/format_comments.csv'
    # transform_coment(output_path, format_coment_path)
    # print(search_similar_coment(format_coment_path, '前排'))
