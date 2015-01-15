package org.menthal.aggregations.tools

/**
 * Created by konrad on 15.01.15.
 */
trait Tree[A] {
  type Forest= List[Tree[A]]
  def reductionsTree[B](z: B)(f: (B, A) => B): Tree[B] = {
    this match {
      case Leaf(x) => Leaf(f(z, x))
      case Node(x, children) => {
        val y = f(z,x)
        val newChildren = for (child <- children) yield child.reductionsTree(y)(f)
        Node(y, newChildren)
      }
    }
  }
  def traverseTree[B](z: B)(f: (B, A) => B): Unit = {
    this match {
      case Leaf(x) =>
        f(z, x)
      case Node(x, children) =>
        val y = f(z, x)
        for (child <- children) child.traverseTree(y)(f)
    }
  }
}
case class Leaf[A](a: A) extends Tree[A]
case class Node[A](a: A, children: List[Tree[A]]) extends Tree[A]
