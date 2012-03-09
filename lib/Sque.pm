use strict;
use warnings;
package Sque;
{
  $Sque::VERSION = '0.001';
}
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
use Net::STOMP::Client;

# ABSTRACT: Background job processing based on Resque, using Stomp

use Sque::Job;
use Sque::Worker;

subtype 'Sugar::Stomp' => as class_type('Net::STOMP::Client');

coerce 'Sugar::Stomp'
    => from 'Str'
    => via {
        my ( $host, $port ) = split /:/;
        my $stomp = Net::STOMP::Client->new( host => $host, port => $port );
        $stomp->connect();
        return $stomp;
    };

has stomp => (
    is => 'ro',
    lazy => 1,
    coerce => 1,
    isa => 'Sugar::Stomp',
    default => sub { Net::STOMP::Client->new->connect },
);

has namespace => ( is => 'rw', default => sub { 'sque' });

has worker => (
    is => 'ro',
    lazy => 1,
    default => sub { Sque::Worker->new( sque => $_[0] ) },
);

sub push {
    my ( $self, $queue, $job ) = @_;
    confess "Can't push an empty job." unless $job;
    $job = $self->new_job($job) unless ref $job eq 'Sque::Job';
    $self->stomp->send(
        persistent => 'true',
        destination => $self->key( $queue ),
        body => $job->encode,
    );
}

sub pop {
    my ( $self ) = @_;
    my $frame = $self->stomp->receive_frame;
    return unless $frame;

    $self->new_job({
        frame => $frame,
        queue => $frame->header('destination'),
    });
}

sub new_job {
    my ( $self, $job ) = @_;

    if ( $job && ref $job && ref $job eq 'HASH' ) {
         return Sque::Job->new({ sque => $self, %$job });
    }
    elsif ( $job ) {
        return Sque::Job->new({ sque => $self, payload => $job });
    }
    confess "Can't build an empty Sque::Job object.";
}

sub key {
    my $self = shift;
    '/queue/' . $self->namespace . '/' . shift;
}


__PACKAGE__->meta->make_immutable();

1;

__END__
=pod

=head1 NAME

Sque - Background job processing based on Resque, using Stomp

=head1 VERSION

version 0.001

=head1 ATTRIBUTES

=head2 stomp

A Stomp Client on this sque instance.

=head2 namespace

Namespace for queues, default is 'sque'

=head2 worker

A L<Sque::Worker> on this sque instance.

=head1 METHODS

=head2 push

Pushes a job onto a queue. Queue name should be a string and the
item should be a Sque::Job object or a hashref containing:
class - The String name of the job class to run.
args - Any arrayref of arguments to pass the job.

Example
$sque->push( archive => { class => 'Archive', args => [ 35, 'tar' ] } )

=head2 pop

Pops a job off a queue. Queue name should be a string.
Returns a Sque::Job object.

=head2 new_job

Build a Sque::Job object on this system for the given
hashref(see Sque::Job) or string(payload for object).

=head2 key

Concatenate $self->namespace with the received array of names
to build a redis key name for this sque instance.

=head1 HELPER METHODS

=head1 AUTHOR

William Wolf <throughnothing@gmail.com>

=head1 COPYRIGHT AND LICENSE


William Wolf has dedicated the work to the Commons by waiving all of his
or her rights to the work worldwide under copyright law and all related or
neighboring legal rights he or she had in the work, to the extent allowable by
law.

Works under CC0 do not require attribution. When citing the work, you should
not imply endorsement by the author.

=cut

