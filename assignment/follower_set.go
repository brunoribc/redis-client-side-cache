package assignment

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/zavitax/sortedset-go"
)

const followersName = "followers"

type RedisSync interface {
	AddAllAndSync(ctx context.Context, set sortedset.SortedSet[string, int64, int64])
	Sync(ctx context.Context)
	Close() error
}

type RedisSyncImpl struct {
	set    *sortedset.SortedSet[string, int64, any]
	client *redis.Client
}

func (f *RedisSyncImpl) addAllAndSync(ctx context.Context) error {
	nodes := f.set.GetRangeByRank(0, -1, false)
	for _, node := range nodes {
		err := f.client.ZAdd(ctx, followersName, redis.Z{Score: float64(node.Score()), Member: node.Key()}).Err()
		if err != nil {
			return err
		}
	}
	return f.Sync(ctx)
}

func (f *RedisSyncImpl) AddAndSync(ctx context.Context, key string, score int64) error {
	err := f.client.ZAdd(ctx, followersName, redis.Z{Score: float64(score), Member: key}).Err()
	if err != nil {
		return err
	}
	return f.Sync(ctx)
}

func (f *RedisSyncImpl) Sync(ctx context.Context) error {
	// sync
	err := f.client.ZCard(ctx, followersName).Err()
	if err != nil {
		return err
	}
	return nil
}

func (f *RedisSyncImpl) init(ctx context.Context, redisOptions *redis.Options, onSync chan bool) error {
	// create invalidations connection and subscribe to invalidate
	var invalidationsClientId int64
	invalidationClientOptions := &redis.Options{
		Addr:     redisOptions.Addr,
		Password: redisOptions.Password,
		DB:       redisOptions.DB,
		Protocol: redisOptions.Protocol,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			var err error
			invalidationsClientId, err = cn.ClientID(ctx).Result()
			if err != nil {
				return err
			}
			return nil
		},
	}

	invalidationsClient := redis.NewClient(invalidationClientOptions)

	sub := invalidationsClient.Subscribe(ctx, "__redis__:invalidate")
	_, err := sub.Receive(ctx) // subscribe message
	ch := sub.Channel()

	err = f.client.Do(ctx, "CLIENT", "TRACKING", "on", "REDIRECT", invalidationsClientId).Err()
	if err != nil {
		return err
	}

	go func() {
		for msg := range ch {
			for _, redisKey := range msg.PayloadSlice {
				switch redisKey {
				case followersName:
					// set changed, sync back
					elements, err := f.client.ZRangeWithScores(ctx, followersName, 0, -1).Result()
					if err != nil {
						onSync <- false
					}
					// removes all elements
					f.set.GetRangeByRank(0, -1, true)
					for _, element := range elements {
						member := element.Member.(string)
						f.set.AddOrUpdate(member, int64(element.Score), struct{}{})
					}
					onSync <- f.Sync(ctx) == nil
				default:
					fmt.Println("Unrecognized key ", redisKey)
				}
			}
		}
		sub.Close()
		invalidationsClient.Close()
	}()

	return nil
}

func NewRedisSyncImpl(ctx context.Context, redisOptions *redis.Options, set *sortedset.SortedSet[string, int64, any], onSync chan bool) (*RedisSyncImpl, error) {
	if redisOptions.Protocol != 2 {
		return nil, fmt.Errorf("redis protocol 2 needs to be used")
	}

	client := redis.NewClient(&redis.Options{
		Protocol: 2,
	})

	redisSyncImpl := &RedisSyncImpl{
		client: client,
		set:    set,
	}

	err := redisSyncImpl.init(ctx, redisOptions, onSync)
	if err != nil {
		redisSyncImpl.Close()
		return nil, err
	}

	err = redisSyncImpl.addAllAndSync(ctx)
	if err != nil {
		redisSyncImpl.Close()
		return nil, err
	}

	return redisSyncImpl, nil
}

func (f *RedisSyncImpl) Close() error {
	return f.client.Close()
}
