import math

def getMax(arr, l, r):
  if l == r:
    return arr[l]

  mid = int((l + r) / 2)
  maxLeft = getMax(arr, l, mid)
  maxRight = getMax(arr, mid + 1, r)
  return max(maxLeft, maxRight)

def main():
  arr = [1, 3, 2, 1, 2, 4]
  # print(len(arr))
  print(getMax(arr, 0, len(arr) - 1))

if __name__ == '__main__':
  main()