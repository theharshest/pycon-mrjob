from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

def jaccard(xs, ys):
    return float(len(set(xs) & set(ys))) / len(set(xs) | set(ys))

class UserSimilarity(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol

    def extract_biz_ids(self, _, record):
        if record['type'] == 'review':
            yield record['user_id'], record['business_id']

    def combine_biz_ids(self, user_id, biz_ids):
        yield user_id, list(set(biz_ids))

    def biz_to_user(self, user_id, biz_ids):
        for biz in biz_ids:
            yield biz, (user_id, biz_ids)

    def calculate_jaccard(self, biz_id, user_defs):
        user_defs_list = list(user_defs)
        for i, user_def in enumerate(user_defs_list):
            for compare_def in user_defs_list[i+1:]:
                score = jaccard(user_def[1], compare_def[1])
                if score >= 0.5:
                    yield (min(user_def[0], compare_def[0]), max(user_def[0],
                            compare_def[0])), score

    def unique_user(self, user_pair, scores):
        yield user_pair, set(scores).pop()

    def steps(self):
        return [self.mr(mapper=self.extract_biz_ids, reducer=self.combine_biz_ids),
                                self.mr(mapper=self.biz_to_user, reducer=self.calculate_jaccard),
                                self.mr(reducer=self.unique_user)]

if __name__ == '__main__':
    UserSimilarity.run()
