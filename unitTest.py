from bisect import bisect_left
from math import floor, sqrt
from typing import List


class Solution:
    def repairCars(self, ranks: List[int], cars: int) -> int:
        l, r = 1, ranks[0] * cars * cars

        def check(m: int) -> bool:
            return sum([floor(sqrt(m // x)) for x in ranks]) >= cars

        while l < r:
            m = l + r >> 1
            if check(m):
                r = m
            else:
                l = m + 1
        return l




so = Solution()
print(so.repairCars(ranks = [5,1,8], cars = 6))

