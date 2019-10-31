#include <cstdlib>
#include <clocale>
#include <vector>
#include <queue>
#include "redismodule.h"

using namespace std;

static uint64_t next_link_n = 100;

static const struct lconv *locale;

int SetRoot(RedisModuleCtx *ctx, RedisModuleString *tree, RedisModuleString *node){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, tree, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_STRING && keytype != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(key);
		return 0;
	}

	if (node == NULL)
		RedisModule_UnlinkKey(key);
	else
		RedisModule_StringSet(key, node);
		
	RedisModule_CloseKey(key);
	return 0;
}

RedisModuleString* GetRoot(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, tree, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_STRING){
		RedisModule_CloseKey(key);
		return NULL;
	}

	size_t len;
	char *ptr = RedisModule_StringDMA(key, &len, REDISMODULE_READ);
	if (ptr == NULL){
		RedisModule_CloseKey(key);
		return NULL;
	}
	
	RedisModuleString *root = RedisModule_CreateString(ctx, ptr, len);
	RedisModule_CloseKey(key);
	return root;
}


int GetNodeCount(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	if (nodelink == NULL) return 0;
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	long long count;
	RedisModuleString *countstr;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS, "count", &countstr, NULL) != REDISMODULE_OK){
		RedisModule_CloseKey(linkkey);
		return 0;
	}
	
	if (RedisModule_StringToLongLong(countstr, &count) != REDISMODULE_OK){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	RedisModule_CloseKey(linkkey);
	return (int)count;
}

int SetNodeCount(RedisModuleCtx *ctx, RedisModuleString *nodelink, int count){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	RedisModuleString *countstr = RedisModule_CreateStringFromLongLong(ctx, (long long)count);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "count", countstr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	
	RedisModule_CloseKey(linkkey);
	return 0;
}

bool IsRedNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	if (nodelink == NULL)
		return false;
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return false;
	}

	bool red = false;
	RedisModuleString *colorstr;
	long long color;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS, "red", &colorstr, NULL) == REDISMODULE_OK)
		if (RedisModule_StringToLongLong(colorstr, &color) == REDISMODULE_OK)
			red = (color != 0) ? true : false;
		
	RedisModule_CloseKey(linkkey);
	return red;
}

int SetRedNode(RedisModuleCtx *ctx, RedisModuleString *nodelink, bool red){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return 0;
	}

	long long lred = (red) ? 1 : 0;
	RedisModuleString *colorstr = RedisModule_CreateStringFromLongLong(ctx, lred);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "red", colorstr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}
RedisModuleString* GetLeft(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}

	RedisModuleString *left = NULL;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS, "left", &left, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}
	RedisModule_CloseKey(linkkey);
	return left;
}

RedisModuleString* GetRight(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}

	RedisModuleString *right = NULL;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS, "right", &right, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return NULL;
	}
	RedisModule_CloseKey(linkkey);
	return right;
}

int SetLeft(RedisModuleCtx *ctx, RedisModuleString *nodelink, RedisModuleString *childlink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	if (childlink == NULL)
		RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "left", REDISMODULE_HASH_DELETE, NULL);
	else if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "left", childlink, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	RedisModule_CloseKey(linkkey);
	return 0;
}


int SetRight(RedisModuleCtx *ctx, RedisModuleString *nodelink, RedisModuleString *childlink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	if (childlink == NULL)
		RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "right", REDISMODULE_HASH_DELETE, NULL);
	else if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "right", childlink, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}
	RedisModule_CloseKey(linkkey);
	return 0;
}


int DeleteRBNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *key = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(key);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(key);
		return -1;
	}
	RedisModule_UnlinkKey(key);
	RedisModule_CloseKey(key);
	return 0;
}
int SetRBNode(RedisModuleCtx *ctx, RedisModuleString *nodelink, int key){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH && keytype != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr = RedisModule_CreateStringFromLongLong(ctx, (long long)key);
	RedisModuleString *countstr = RedisModule_CreateStringFromLongLong(ctx, (long long)1);
	RedisModuleString *redstr = RedisModule_CreateStringFromLongLong(ctx, (long long)1);

	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS,
							"key", keystr,"count", countstr,"red", redstr, NULL) != 3){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

bool IsNodeNull(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype == REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(linkkey);
		return true;
	}
	RedisModule_CloseKey(linkkey);
	return false;
}

int GetKey(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_READ);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr;
	if (RedisModule_HashGet(linkkey, REDISMODULE_HASH_CFIELDS, "key", &keystr, NULL) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	long long key;
	if (RedisModule_StringToLongLong(keystr, &key) == REDISMODULE_ERR){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return (int)key;
}

int SetKey(RedisModuleCtx *ctx, RedisModuleString *nodelink, int key){
	RedisModuleKey *linkkey = (RedisModuleKey*)RedisModule_OpenKey(ctx, nodelink, REDISMODULE_WRITE);
	int keytype = RedisModule_KeyType(linkkey);
	if (keytype != REDISMODULE_KEYTYPE_HASH){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModuleString *keystr = RedisModule_CreateStringFromLongLong(ctx, (long long)key);
	if (RedisModule_HashSet(linkkey, REDISMODULE_HASH_CFIELDS, "key", keystr, NULL) != 1){
		RedisModule_CloseKey(linkkey);
		return -1;
	}

	RedisModule_CloseKey(linkkey);
	return 0;
}

int FlipNode(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	return SetRedNode(ctx, nodelink, !IsRedNode(ctx, nodelink));
}


int FlipNodeAndChildNodes(RedisModuleCtx *ctx, RedisModuleString *nodelink){
	FlipNode(ctx, nodelink);

	RedisModuleString *left = GetLeft(ctx, nodelink);
	FlipNode(ctx, left);
	
	RedisModuleString *right = GetRight(ctx, nodelink);
	FlipNode(ctx, right);

	return 0;
}

RedisModuleString* RotateLeft(RedisModuleCtx *ctx, RedisModuleString *h){
	RedisModuleString *x = GetRight(ctx, h);
	RedisModuleString *xleft = GetLeft(ctx, x);
	
	SetRight(ctx, h, xleft);
   	SetLeft(ctx, x, h);

	SetRedNode(ctx, x, IsRedNode(ctx, h));
	SetRedNode(ctx, h, true);

	RedisModuleString *hleft = GetLeft(ctx, h);
	RedisModuleString *hright = GetRight(ctx, h);
	
	SetNodeCount(ctx, x, GetNodeCount(ctx, h));
	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx,hleft) + GetNodeCount(ctx, hright));

	return x;
}

RedisModuleString* RotateRight(RedisModuleCtx *ctx, RedisModuleString *h){
	RedisModuleString *x = GetLeft(ctx, h);

	RedisModuleString *xright = GetRight(ctx, x);
	SetLeft(ctx, h, xright);
	SetRight(ctx, x, h);

	SetRedNode(ctx, x, IsRedNode(ctx, h));
	SetRedNode(ctx, h, true);

	RedisModuleString *hleft = GetLeft(ctx, h);
	RedisModuleString *hright = GetRight(ctx, h);
	
	SetNodeCount(ctx, x, GetNodeCount(ctx, h));
	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx, hleft) + GetNodeCount(ctx, hright));

	return x;
}

RedisModuleString* balance(RedisModuleCtx *ctx, RedisModuleString *h){

	if (IsRedNode(ctx, GetRight(ctx, h)))
		h = RotateLeft(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, h)) && IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = RotateRight(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, h)) && IsRedNode(ctx, GetRight(ctx, h)))
		FlipNodeAndChildNodes(ctx, h);

	SetNodeCount(ctx, h, 1 + GetNodeCount(ctx, GetLeft(ctx, h)) + GetNodeCount(ctx, GetRight(ctx, h)));
	return h;
}



RedisModuleString* rbtree_insert(RedisModuleCtx *ctx, RedisModuleString *node, int key, int level = 0){
	if (node == NULL){
		RedisModuleString *linkstr = RedisModule_CreateStringPrintf(ctx, "node-%ld", next_link_n);
		RedisModule_Log(ctx, "debug", "(level %d) insert new link - %s",
						level, RedisModule_StringPtrLen(linkstr, NULL));
		next_link_n += 100;
		SetRBNode(ctx, linkstr, key);
		return linkstr;
	}

	RedisModuleString *left = GetLeft(ctx, node);
	RedisModuleString *right = GetRight(ctx, node);
	
	int node_key = GetKey(ctx, node);
	RedisModule_Log(ctx, "debug", "(level %d) insert %d node %d", level, key, node_key);
	if (key < node_key){
		SetLeft(ctx, node, rbtree_insert(ctx, left, key, level+1));
	} else if (key > node_key){
		SetRight(ctx, node, rbtree_insert(ctx, right, key, level+1));
	}

	left = GetLeft(ctx, node);
	right = GetRight(ctx, node);

	if (IsRedNode(ctx, right) && !IsRedNode(ctx, left)){
		RedisModule_Log(ctx, "debug", "(level %d) right is red, left is black - rotate left", level);
		node = RotateLeft(ctx, node);
	}

	left = GetLeft(ctx, node);
	RedisModuleString *leftleft = GetLeft(ctx, left);
	right = GetRight(ctx, node);

	if (IsRedNode(ctx, left) && IsRedNode(ctx, leftleft)){
		RedisModule_Log(ctx, "debug", "(level %d) left is red, left left is red - rotate right", level);
		node = RotateRight(ctx, node);
	}

	left = GetLeft(ctx, node);
	right = GetRight(ctx, node);
	if (IsRedNode(ctx, left) && IsRedNode(ctx, right)){
		RedisModule_Log(ctx, "debug", "(level %d) left, right both red - flip", level);
		FlipNodeAndChildNodes(ctx, node);
	}

	SetNodeCount(ctx, node, 1 + GetNodeCount(ctx, left) + GetNodeCount(ctx, right));
	return node;
}

int RBTreeInsert(RedisModuleCtx *ctx, RedisModuleString *tree, int key){
	RedisModuleString *root = GetRoot(ctx, tree);
	RedisModule_Log(ctx, "debug", "tree %s root %s",
					RedisModule_StringPtrLen(tree, NULL),
					RedisModule_StringPtrLen(root, NULL));
	RedisModuleString *node = rbtree_insert(ctx, root, key, 0);
	SetRoot(ctx, tree, node);
	SetRedNode(ctx, node, false);
    return 0;
}

int RBTreeLookup(RedisModuleCtx *ctx, RedisModuleString *tree, const int key){
	RedisModuleString *node = GetRoot(ctx, tree);
	int node_key = -1;
	while (node != NULL){
		int current_key = GetKey(ctx, node);
		if (current_key == key){
			node_key = current_key;
			break;
		}
		if (key < current_key){
			node = GetLeft(ctx, node);
		} else {
			node = GetRight(ctx, node);
		}
	}
	return node_key;
}

RedisModuleString* minimum(RedisModuleCtx *ctx, RedisModuleString *h){
	while (h != NULL && GetLeft(ctx, h) != NULL)
		h = GetLeft(ctx, h);
	return h;
}

RedisModuleString* MoveRedLeft(RedisModuleCtx *ctx, RedisModuleString *h){
	FlipNodeAndChildNodes(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, GetRight(ctx, h)))){
		SetRight(ctx, h, RotateRight(ctx, GetRight(ctx, h)));
		h = RotateLeft(ctx, h);
	}
	return h;
}

RedisModuleString* MoveRedRight(RedisModuleCtx *ctx, RedisModuleString *h){
	FlipNodeAndChildNodes(ctx, h);
	if (IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = RotateRight(ctx, h);
	return h;
}

RedisModuleString* delete_minimum(RedisModuleCtx *ctx, RedisModuleString *h){
	if (GetLeft(ctx, h) == NULL){
		DeleteRBNode(ctx, h);
		return NULL;
	}
	if (!IsRedNode(ctx, GetLeft(ctx, h)) && !IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
		h = MoveRedLeft(ctx, h);
	SetLeft(ctx, h, delete_minimum(ctx, GetLeft(ctx, h)));
	return balance(ctx, h);
}

void RBTreeDeleteMin(RedisModuleCtx *ctx, RedisModuleString *tree){
	if (tree == NULL || GetRoot(ctx, tree) == NULL)
		return;

	RedisModuleString *root = GetRoot(ctx, tree);
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);
	root = delete_minimum(ctx, root);
	SetRoot(ctx, tree, root);
	if (root != NULL)
		SetRedNode(ctx, root, false);
	return;
}

RedisModuleString* delete_key(RedisModuleCtx *ctx, RedisModuleString *h, int key){
	if (key < GetKey(ctx, h)){
		if (!IsRedNode(ctx, GetLeft(ctx, h)) && !IsRedNode(ctx, GetLeft(ctx, GetLeft(ctx, h))))
			h = MoveRedLeft(ctx, h);
		SetLeft(ctx, h, delete_key(ctx, GetLeft(ctx, h), key));
	} else {
		if (IsRedNode(ctx, GetLeft(ctx, h)))
			h = RotateRight(ctx, h);
		if (key == GetKey(ctx, h) && GetRight(ctx, h) == NULL){
			DeleteRBNode(ctx, h);
			return NULL;
		}
		if (!IsRedNode(ctx, GetRight(ctx, h)) && !IsRedNode(ctx,GetLeft(ctx, GetRight(ctx, h))))
			h = MoveRedRight(ctx, h);
		if (key == GetKey(ctx, h)){
			RedisModuleString *x = minimum(ctx, GetRight(ctx, h));
			SetKey(ctx, h, GetKey(ctx, x));
			SetNodeCount(ctx, h, GetNodeCount(ctx, x));
			SetRight(ctx, h, delete_minimum(ctx, GetRight(ctx, h)));
		} else {
			SetRight(ctx, h, delete_key(ctx, GetRight(ctx, h), key));
		}
	}
	return balance(ctx, h);
}

void RBTreeDeleteKey(RedisModuleCtx *ctx, RedisModuleString *tree, int key){
	if (tree == NULL || GetRoot(ctx, tree) == NULL)
		return;
	RedisModuleString *root = GetRoot(ctx, tree);
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);
	root = delete_key(ctx, root, key);
	SetRoot(ctx, tree, root);
	if (root != NULL)
		SetRedNode(ctx, root, false);
	return;
}


RedisModuleString* delete_maximum(RedisModuleCtx *ctx, RedisModuleString *h){
	if (IsRedNode(ctx, GetLeft(ctx, h)))
		h = RotateRight(ctx, h);
	if (GetRight(ctx, h) == NULL){
		DeleteRBNode(ctx, h);
		return NULL;
	}
	if (!IsRedNode(ctx, GetRight(ctx, h)) && !IsRedNode(ctx, GetLeft(ctx, GetRight(ctx, h))))
		h = MoveRedRight(ctx, h);
	SetRight(ctx, h, delete_maximum(ctx, GetRight(ctx, h)));
	return balance(ctx, h);
}

void RBTreeDeleteMax(RedisModuleCtx *ctx, RedisModuleString *tree){
	if (tree == NULL || GetRoot(ctx, tree) == NULL)
		return;
	RedisModuleString *root = GetRoot(ctx, tree);
	if (!IsRedNode(ctx, GetLeft(ctx, root)) && !IsRedNode(ctx, GetRight(ctx, root)))
		SetRedNode(ctx, root, true);
				   
	root = delete_maximum(ctx, root);
	if (root != NULL)
		SetRedNode(ctx, root, false);
	SetRoot(ctx, tree, root);
	return;
}


int RBTreeClear(RedisModuleCtx *ctx, RedisModuleString *tree){
	queue<RedisModuleString*> q;

	RedisModuleString *root = GetRoot(ctx, tree);
	if (tree != NULL)
		q.push(root);

	int count = 0;
	while (!q.empty()){
		RedisModuleString *node = q.front();
		count++;
		
		RedisModuleString *left = GetLeft(ctx, node);
		if (left != NULL)
			q.push(left);
		RedisModuleString *right = GetRight(ctx, node);
		if (right != NULL)
			q.push(right);
		q.pop();
		DeleteRBNode(ctx, node);
	}
	SetRoot(ctx, tree, NULL);
	return count;
}

void logtree(RedisModuleCtx *ctx, RedisModuleString *tree){
	queue<RedisModuleString*> q;

	RedisModuleString *root = GetRoot(ctx, tree);
	if (root != NULL)
		q.push(root);

	int level = 0;
	while (!q.empty()){
		int sz = q.size();

		RedisModule_Log(ctx, "debug", "level %d", level);
		
		for (int i=0;i<sz;i++){
			if (IsRedNode(ctx, q.front())){
				RedisModule_Log(ctx, "debug", "%s (key%d RED)",
								RedisModule_StringPtrLen(q.front(), NULL), GetKey(ctx, q.front()));
			} else {
				RedisModule_Log(ctx, "debug", "%s (key=%d)",
								RedisModule_StringPtrLen(q.front(), NULL), GetKey(ctx, q.front()));
			}

			RedisModuleString *left = GetLeft(ctx, q.front());
			if (left != NULL)
				q.push(left);

			RedisModuleString *right = GetRight(ctx, q.front());
			if (right != NULL)
				q.push(right);

			q.pop();
		}
		level++;
	}
	
}

int RBTreeSize(RedisModuleCtx *ctx, RedisModuleString *tree){
	RedisModuleString *root = GetRoot(ctx, tree);
	if (root == NULL)
		return 0;
	return GetNodeCount(ctx, root);
}


extern "C" int RBTreeInsert_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	RedisModule_AutoMemory(ctx);
	if (argc <= 3)
		return RedisModule_WrongArity(ctx);
	
	RedisModuleString *tree_name = argv[1];

	for (int i=2;i<argc;i++){
		long long key;
		if (RedisModule_StringToLongLong(argv[i], &key) == REDISMODULE_ERR){
			return REDISMODULE_ERR;
		}
		int ikey = (int)key;
		RedisModule_Log(ctx, "debug", "rbtree: recieved %s %d", RedisModule_StringPtrLen(tree_name, NULL), ikey);

		if (RBTreeInsert(ctx, tree_name, ikey) < 0){
			RedisModule_ReplyWithError(ctx, "ERR unable to insert");
			return REDISMODULE_ERR;
		}
	}
	
	RedisModule_ReplyWithLongLong(ctx, (long long)argc-2);
	return REDISMODULE_OK;
}

extern "C" int RBTreeLog_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RedisModuleString *tree_name = argv[1];
	logtree(ctx, tree_name);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

extern "C" int RBTreeSize_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RedisModuleString *tree_name = argv[1];
	long long sz = (long long)RBTreeSize(ctx, tree_name);
	RedisModule_ReplyWithLongLong(ctx, sz);
	return REDISMODULE_OK;
}


extern "C" int RBTreeClear_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	long long n = (long long)RBTreeClear(ctx, argv[1]);
	RedisModule_ReplyWithLongLong(ctx, n);
	return REDISMODULE_OK;
}

extern "C" int RBTreeDeleteMin_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RBTreeDeleteMin(ctx, argv[1]);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

extern "C" int RBTreeDeleteMax_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 2)
		return RedisModule_WrongArity(ctx);
	RBTreeDeleteMax(ctx, argv[1]);
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

extern "C" int RBTreeLookup_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc != 3)
		return RedisModule_WrongArity(ctx);

	long long key;
	if (RedisModule_StringToLongLong(argv[2], &key) == REDISMODULE_ERR){
		RedisModule_ReplyWithError(ctx, "ERR unable to complete lookup");
		return REDISMODULE_ERR;
	}
	
	long long rkey = (long long)RBTreeLookup(ctx, argv[1], (int)key);
	RedisModule_ReplyWithLongLong(ctx, rkey);
	return REDISMODULE_OK;
}

extern "C" int RBTreeDeleteKey_RedisCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (argc < 3)
		return RedisModule_WrongArity(ctx);

	for (int i=2;i<argc;i++){
		long long key;
		if (RedisModule_StringToLongLong(argv[i], &key) == REDISMODULE_ERR)
			continue;
		RBTreeDeleteKey(ctx, argv[1], (int)key);
	}
	RedisModule_ReplyWithSimpleString(ctx, "OK");
	return REDISMODULE_OK;
}

extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
	if (RedisModule_Init(ctx, "rbtree", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	locale = localeconv();
	
	if (RedisModule_CreateCommand(ctx, "rbtree.insert",  RBTreeInsert_RedisCmd,
								  "write fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.tree2log", RBTreeLog_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.count", RBTreeSize_RedisCmd,
								  "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.clear", RBTreeClear_RedisCmd,
								  "write deny-oom", 1, -1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.delmin", RBTreeDeleteMin_RedisCmd,
								  "write fast deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
								  

	if (RedisModule_CreateCommand(ctx, "rbtree.delmax", RBTreeDeleteMax_RedisCmd,
								  "write fast deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.del", RBTreeDeleteKey_RedisCmd,
								  "write fast deny-oom", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;

	if (RedisModule_CreateCommand(ctx, "rbtree.lookup", RBTreeLookup_RedisCmd,
								  "readonly", 1, 1, 1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;
	
	return REDISMODULE_OK;
}

