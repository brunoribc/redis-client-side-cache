package assignment

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/zavitax/sortedset-go"
	"testing"
)

func TestFollowerSet(t *testing.T) {
	ctx := context.Background()
	options := &redis.Options{
		Addr:     "localhost:6379",
		Protocol: 2,
	}
	otherRedisclient := redis.NewClient(options)
	defer otherRedisclient.Close()

	// redis clean
	otherRedisclient.Del(ctx, followersName)

	t.Run("should keep local set in sync with redis set", func(t *testing.T) {
		// create sorted set
		empty := struct{}{}
		set := sortedset.New[string, int64, any]()
		set.AddOrUpdate("bruno", 3, empty)
		set.AddOrUpdate("boris", 1, empty)
		set.AddOrUpdate("tommaso", 2, empty)

		// redis sync
		setSynced := make(chan bool)
		redisSync, err := NewRedisSyncImpl(ctx, options, set, setSynced)
		assert.NoError(t, err)
		defer redisSync.Close()

		assert.NotNil(t, set.GetByKey("boris"))
		// redis changed
		exists, err := otherRedisclient.ZRem(ctx, followersName, "boris").Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), exists)

		// expected invalidation received
		syncOk := <-setSynced
		assert.True(t, syncOk)
		assert.Nil(t, set.GetByKey("boris"))

		var followersLeft []string
		set.IterFuncRangeByRank(1, -1, func(key string, value any) bool {
			followersLeft = append(followersLeft, key)
			return true
		})
		assert.Equal(t, []string{"tommaso", "bruno"}, followersLeft)

		// redis change rank
		rank, err := otherRedisclient.ZIncrBy(ctx, followersName, 5, "tommaso").Result()
		assert.NoError(t, err)

		syncOk = <-setSynced
		assert.True(t, syncOk)
		assert.Equal(t, int64(7), int64(rank))
		assert.Equal(t, int64(7), set.GetByKey("tommaso").Score())

		// new order
		followersLeft = []string{}
		set.IterFuncRangeByRank(1, -1, func(key string, value any) bool {
			followersLeft = append(followersLeft, key)
			return true
		})
		assert.Equal(t, []string{"bruno", "tommaso"}, followersLeft)
	})
}
