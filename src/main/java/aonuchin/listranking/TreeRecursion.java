package aonuchin.listranking;

public interface TreeRecursion<N extends TreeNode, T> {
  T accumulateChildNode(T nodeValue, T accumulator);
  T accumulateNode(N node, T accumulator);
  T initialValue();
}
