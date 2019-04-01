import threading
import xml
import requests
import numpy
from xml.parsers.expat import ParserCreate


class MySaxHandler:
    def __init__(self):
        self.p_barrage_list = []
        self.barrage = ''

    def start_element(self, name, attrs):
        if name == 'd':
            self.barrage = attrs['p'] + ','

    def end_element(self, name):
        if name == 'd':
            self.p_barrage_list.append(self.barrage)
            self.barrage = ''

    def char_data(self, text):
        self.barrage += text

    def get_result(self):
        r = self.p_barrage_list
        return r


write_lock = threading.Lock()


def run_all(thread_num, start_offset, end_offset):
    """
    :param thread_num: start threads for the application
    :param start_offset: vedio cid start offset
    :param end_offset: vedio cid end offset
    :return:
    """
    per_thread_tasks = numpy.math.ceil((end_offset - start_offset + 1) / thread_num)
    thread_list = []
    for i in range(thread_num):
        t = i < thread_num - 1 and threading.Thread(target=split_task, args=(
            list(range(i * per_thread_tasks, (i + 1) * per_thread_tasks)),)) or threading.Thread(
            target=split_task, args=(list(range(i * per_thread_tasks, end_offset + 1)),))
        thread_list.append(t)
        t.start()
    for t in thread_list:
        t.join()


def split_task(cid_list):
    """
    :param cid_list: cid list
    :return:
    """
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
    output_path = 'bilibili_barrage_dir'
    output_partitions = 10
    part_list_prefix = 'part_'
    for i in range(output_partitions):
        locals()[part_list_prefix + str(i)] = []
    handler = MySaxHandler()
    comment = []
    barrage_list = []
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
        try:
            parser.Parse(r.content)
        except xml.parsers.expat.ExpatError:
            print(r.content.decode(encoding='utf-8', errors='ignore'))

        barrage_list.extend(handler.get_result())
        if len(barrage_list) == output_partitions * 1000:
            print('satified dump condition')
            for barrage in barrage_list:
                locals()[part_list_prefix + str(int(float(barrage.split(',')[0]) % output_partitions))].append(barrage)
            barrage_list.clear()
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
    if len(barrage_list) > 0:
        print('dump the leave data to disk')
        for barrage in barrage_list:
            locals()[part_list_prefix + str(int(float(barrage.split(',')[0]) % output_partitions))].append(barrage)
        for i in range(output_partitions):
            write_lock.acquire()
            try:
                with open(output_path + '/part_' + str(i) + '.csv', 'a', encoding='utf-8') as fa:
                    tuple(map(lambda c: fa.write(c + '\n'), locals()[part_list_prefix + str(i)]))
            finally:
                write_lock.release()


def comment_count(path):
    """
    :param path: barrage file path
    :return: barrage world count
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


def transform_barrage(basepath, resultpath):
    """
    :param basepath: base barrage file path
    :param resultpath: transform result path
    :return:
    """
    format_barrage_list = []
    with open(basepath, 'r', encoding='utf-8', errors='ignore') as fr:
        for line in fr:
            if len(format_barrage_list) == 1000:
                with open(resultpath, 'a', encoding='utf-8') as fa:
                    tuple(map(lambda w: fa.write(w + '\n'), format_barrage_list))
                    format_barrage_list.clear()
            split_word = line.split(',')
            if len(split_word) < 8:
                continue
            format_barrage_list.append(','.join(split_word[8:]))


def search_similar_barrage(path, keyword):
    """
    :param path: barrage file path
    :param keyword: key word to get similar barrage
    :return: similar barrage
    """
    similar_barrage_list = []
    with open(path, 'r', encoding='utf-8', errors='ignore') as fr:
        for line in fr:
            if keyword in line:
                similar_barrage_list.append(line)
    similar_barrage_dict = dict()
    for w in similar_barrage_list:
        similar_barrage_dict[w] = w in similar_barrage_dict and similar_barrage_dict[w] + 1 or 1
    similar_barrage_count = sorted(similar_barrage_dict.items(), key=lambda d: d[1], reverse=True)
    return similar_barrage_count


if __name__ == '__main__':
    output_path = 'bilibili_barrage_dir'
    run_all(14, 1, 10000)
    # run(10000, 50, output_path)
    # comment_count(output_dir)
    # format_barrage_path = 'bilibili_barrage_dir/format_comments.csv'
    # transform_barrage(output_path, format_barrage_path)
    # print(search_similar_barrage(format_barrage_path, 'ab'))
