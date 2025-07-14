我们复用了Geth的实现。

【主网数据集来源】
我们利用全节点获取了交易和区块信息，获取方法为使用ethereum-etl工具 （网址：https://github.com/blockchain-etl/ethereum-etl）
我们利用命令ethereumetl export_blocks_and_transactions --start-block 0 --end-block 1000000 --blocks-output E:\ethdata\blocks_1.csv --transactions-output E:\ethdata\transactions_1.csv  --provider-uri http://[ip]:[port]
从全节点将交易和区块信息进行了下载。

【实验测试用例】
我们在两个位置进行实验，一个是主网数据实验，在processor_eth_compare_test文件
（备注：我们在在processor_eth_test文件中进行了一些bug查找。）

第二个是压力测试，我们分别在sliding_test、verkle_sliding_test、cache_trie_test分别进行了MPT、verkle tree和SWMT的实验。

【代码修改】
我们在CacheTrie文件夹中完成了SWMT的实现。其中cache_trie.go主要为滑动窗口协议的实现,crowd_window.go主要为拥塞回避协议的实现。
我们在Core/State/Reader.go中实现了CacheTrieReader，该类可以在snapshot查询前进行SWMT的缓存查询。
我们在Trie/cache_proxy_trie.go中，实现了对写入的代理封装，当database开启cache模式后，就会优先使用SWMT进行数据写入。
我们在statedb.go中完成了对SWMT模式的处理，当开启了cache模式后，不会直接操作底层的global state tree
最后我们在测试用例processor_eth_compare_test.go中，完成了重新运行区块的逻辑，并根据是否开启SWMT,决定是否进行延迟写得逻辑。

【运行指南】
首先需要使用ethereumetl命令下载数据集，因为区块数据太多的原因，我们只提供了前100w个区块的数据。
然后修改processor_eth_compare_test.go文件中的运行范围。
最后运行TestCompareProcessTransactions测试用例。
配置项 tc.go 硬编码
cacheTrie ： 是否使用SWMT模式
verkleTree ： 是否使用verkle树