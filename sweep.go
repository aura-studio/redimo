package redimo

import (
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Orphan sweep (fork v1.7 extension, redimos task 11.2).
//
// SweepOrphans is the storage primitive behind the proxy's weekly sweeper. The lazy
// deleter (see members.go / DeleteMembers) handles the common delete path, but it
// is best-effort: pks dropped by a full delete queue, or left behind by a member
// reclaim that failed, would otherwise linger as "orphan" data members whose owning
// key has no meta item. The read path never returns them (a missing meta item makes
// the key logically absent), but they still consume storage. The weekly sweep
// rescans the whole table and reclaims them.

// SweepOrphans scans the entire table for orphan data members — items whose owning
// partition key (pk) has no meta item (sk = "#meta") — and reclaims them.
//
// It pages through the table with Scan (projecting only the key attributes),
// grouping the items by pk to learn which pks carry a meta item and which data
// members belong to each pk. Any pk that has data members but no meta item is an
// orphan; its members are removed with BatchWriteItem in batches of batchSize
// (retrying UnprocessedItems). It returns the number of orphan members submitted
// for deletion.
//
// batchSize is clamped to [1, MaxBatchWriteItems]; a value <= 0 selects the
// DynamoDB per-call maximum. A pk that carries a meta item is left entirely
// untouched, so a key that is merely mid-write (meta already present, members still
// arriving) is never disturbed. Scan reads are eventually consistent, matching the
// backstop, best-effort nature of the sweep: anything missed is caught on the next
// weekly run.
func (c Client) SweepOrphans(batchSize int) (reclaimed int, err error) {
	if batchSize <= 0 || batchSize > MaxBatchWriteItems {
		batchSize = MaxBatchWriteItems
	}

	// hasMeta[pk] is true once a "#meta" item has been seen for pk; members[pk]
	// accumulates the pk's data-member keys. Grouping is required because Scan does
	// not guarantee items sharing a pk arrive together or that a pk's meta item is
	// seen before its members.
	hasMeta := make(map[string]bool)
	members := make(map[string][]keyDef)

	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		resp, serr := c.ddbClient.Scan(c.context(), &dynamodb.ScanInput{
			ExclusiveStartKey:    lastEvaluatedKey,
			ProjectionExpression: aws.String(strings.Join([]string{c.partitionKey, c.sortKey}, ", ")),
			TableName:            aws.String(c.tableName),
		})
		if serr != nil {
			return reclaimed, serr
		}

		for _, item := range resp.Items {
			k := parseKey(item, c)
			if c.isMetaItem(item) {
				hasMeta[k.pk] = true
				// Drop any members already collected for this pk: it is not an orphan.
				delete(members, k.pk)

				continue
			}

			if hasMeta[k.pk] {
				// Meta already seen for this pk; the member is live, skip it.
				continue
			}

			members[k.pk] = append(members[k.pk], k)
		}

		if len(resp.LastEvaluatedKey) == 0 {
			break
		}

		lastEvaluatedKey = resp.LastEvaluatedKey
	}

	// Every pk left in members has data items but no meta item: reclaim it. A pk whose
	// meta item was seen at any point had its members entry deleted (or was never
	// collected), so nothing here is a live key — no per-pk hasMeta re-check is needed.
	for _, keys := range members {
		n, derr := c.batchDeleteKeys(keys, batchSize)
		reclaimed += n

		if derr != nil {
			return reclaimed, derr
		}
	}

	return reclaimed, nil
}
